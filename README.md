# NealeLab 2018 UK Biobank GWAS Reproduction Pipeline

This pipeline is a WIP, but it will attempt to reproduce this [GWAS](http://www.nealelab.is/uk-biobank) (with associated code at [UK_Biobank_GWAS](https://github.com/Nealelab/UK_Biobank_GWAS)) using [sgkit](https://github.com/pystatgen/sgkit).

## Local Setup

To run this pipeline, all that is necessary is a local workstation with 24G RAM and 100G disk.
The local workstation will run some pipeline steps while others will be launched via snakemake on a GKE cluster.
While it is possible to run these commands on a VM outside of GCP, it is generally easier if you do so from one within it.

The local snakemake python environment should be initialized as follows:

```bash
conda env create -f envs/snakemake.yaml 
conda activate snakemake
```

## Cluster Management

This pipeline involves steps that require very different resource profiles.  Because of this, 
certain phases of the pipeline will require an appropriately defined GKE cluster.  These 
clusters should be created/modified/deleted when necessary as they can be very expensive,
and the commands below show how to do that.

### Create Cluster

To create a GKE cluster that snakemake can execute rules on, follow these steps:

```bash
source env.sh; source .env

gcloud init

gcloud components install kubectl

gcloud config set project "$GCP_PROJECT"

# Create cluster with 8 vCPUs/32GiB RAM/200G disk per node
# Memory must be multiple of 256 MiB (argument is MiB)
gcloud container clusters create \
  --machine-type custom-${GKE_IO_NCPU}-32768 \
  --disk-type pd-standard \
  --disk-size 200G \
  --num-nodes 2 \
  --zone $GCP_ZONE \
  --node-locations $GCP_ZONE \
  --cluster-version latest \
  --scopes storage-rw \
  $GKE_IO_NAME

# Grant admin permissions on cluster
gcloud container clusters get-credentials $GKE_IO_NAME --zone $GCP_ZONE
kubectl create clusterrolebinding cluster-admin-binding \
  --clusterrole=cluster-admin \
  --user=$GCP_USER_EMAIL
  
# Note: If you see this, add IAM policy as below
# Error from server (Forbidden): clusterrolebindings.rbac.authorization.k8s.io is forbidden: 
# User "XXXXX" cannot create resource "clusterrolebindings" in API group "rbac.authorization.k8s.io" 
# at the cluster scope: requires one of ["container.clusterRoleBindings.create"] permission(s).
gcloud projects add-iam-policy-binding $GCP_PROJECT \
  --member=user:$GCP_USER_EMAIL \
  --role=roles/container.admin
  
# Login for GS Read/Write in pipeline rules
gcloud auth application-default login

# Run snakemake commands
```


### Modify Cluster

```bash
source env.sh; source .env

## Resize
gcloud container clusters resize $GKE_IO_NAME --node-pool default-pool --num-nodes 2 --zone $GCP_ZONE

## Get status
kubectl get node # Find node name
gcloud compute ssh gke-ukb-io-default-pool-XXXXX

## Remove the cluster
gcloud container clusters delete $GKE_IO_NAME --zone $GCP_ZONE

## Remove node from cluster
kubectl get nodes
# Find node to delete: gke-ukb-io-1-default-pool-276513bc-48k5
kubectl drain gke-ukb-io-1-default-pool-276513bc-48k5 --force --ignore-daemonsets
gcloud container clusters describe ukb-io-1 --zone us-east1-c 
# Find instance group name: gke-ukb-io-1-default-pool-276513bc-grp
gcloud compute instance-groups managed delete-instances gke-ukb-io-1-default-pool-276513bc-grp --instances=gke-ukb-io-1-default-pool-276513bc-48k5 --zone us-east1-c 
```


## Execution

All of the following should be run from the root directory from this repo. 

```bash
# Run this first before any of the steps below
conda activate snakemake
source env.sh; source .env
```

## Main UKB dataset integration

```bash
# Convert main dataset to parquet
# Takes ~45 mins on 4 cores, 12g heap
snakemake --use-conda --cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep-data/main/ukb.ckpt
    
# Extract sample QC from main dataset (as zarr)
snakemake --use-conda --cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep-data/main/ukb_sample_qc.ckpt
    
# Extract sample sets
snakemake --use-conda --cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/pipe-data/external/nealelab_v3_20180731/extract/sample_sets.csv

# Download data dictionary
snakemake --use-conda --cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/pipe-data/external/ukb_meta/data_dictionary_showcase.csv
```

## Phenotype Prep

These steps can be run locally, but the local machine must be resized
to have at least 200G RAM.  They can alternatively be run on a GKE
cluster by adding `--kubernetes` to the commands below.

```bash
# Create the input PHESANT phenotype CSV (takes ~15 mins)
snakemake --use-conda --cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep-data/main/ukb_phesant_prep.csv
    
# Create the phenotype subset to be used for validation
snakemake --use-conda --cores=1 \
    repos/PHESANT/variable-info/outcome_info_final_pharma_nov2019.tsv-subset01.tsv
    
# Generate the normalized phenotype data (takes several hours)
snakemake --use-conda --cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep-data/main/ukb_phesant_phenotypes-subset01.csv
```

### Zarr Integration


```bash

# Create cluster with 8 vCPUs/32GiB RAM/200G disk per node
gcloud container clusters create \
  --machine-type custom-${GKE_IO_NCPU}-${GKE_IO_MEM_MB} \
  --disk-type pd-standard \
  --disk-size ${GKE_IO_DISK_GB}G \
  --num-nodes 2 \
  --zone $GCP_ZONE \
  --node-locations $GCP_ZONE \
  --cluster-version latest \
  --scopes storage-rw \
  $GKE_IO_NAME
  
# Generate single zarr archive from bgen
# * Set local cores to 1 so that only one rule runs at a time on cluster hosts
snakemake --kubernetes --use-conda --local-cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep-data/gt-imputation/ukb_chrXY.ckpt
# Expecting running time: ~40 minutes

# Resize cluster and run on more files:
gcloud container clusters resize $GKE_IO_NAME --node-pool default-pool --num-nodes 2 --zone $GCP_ZONE
snakemake --kubernetes --use-conda --cores=2 --local-cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep-data/gt-imputation/ukb_chr{21,22}.ckpt
# Expecting running time w/ 8 vCPUs/32GiB RAM/200G disk per node: 12 - 14 hours

# Delete the GKE cluster
```


## Analysis

WIP docs on how to run the actual GWAS.


First create Dask cluster, see:

- https://docs.dask.org/en/latest/setup/kubernetes-helm.html
- https://zero-to-jupyterhub.readthedocs.io/en/latest/google/step-zero-gcp.html

\* fully distributed dask cluster not working yet

```
source env.sh; source .env
gcloud container clusters create \
  --machine-type n1-highmem-4 \
  --num-nodes 1 \
  --zone $GCP_ZONE \
  --node-locations $GCP_ZONE \
  --cluster-version latest \
  --scopes storage-rw \
  $GKE_DASK_NAME
  
kubectl create clusterrolebinding cluster-admin-binding \
  --clusterrole=cluster-admin \
  --user=$GCP_USER_EMAIL
  
gcloud container clusters get-credentials $GKE_DASK_NAME --zone $GCP_ZONE

# Install Helm (follow https://helm.sh/docs/intro/install/#from-script)

helm repo add dask https://helm.dask.org/
helm repo update

# This will deploy the GKE services on the existing cluster
# Note: by default, external ips are no longer exposed (see https://stackoverflow.com/questions/62324275/external-ip-not-exposed-helm-dask)
# but this can be configured with scheduler.serviceType
# helm install dask/dask --generate-name # Get generic name
helm install ukb-dask-helm-1 dask/dask -f config/dask/helm.yaml

# Get environment variables and general status message
helm status ukb-dask-helm-1

helm get all ukb-dask-helm-1

# To upgrade packages installed or change worker resources:
helm upgrade ukb-dask-helm-1 dask/dask -f config/dask/helm.yaml
# Note that this will work on a live cluster -- no port forwards
# or other processes need to be restarted (even notebook kernels)

# To manually scale workers:
gcloud container clusters resize ukb-dask-1 --node-pool default-pool --num-nodes 8 --zone $GCP_ZONE
kubectl scale deployment/ukb-dask-helm-1-worker --replicas=2

# Use this to show current cpu/memory allocation on nodes:
kubectl describe nodes

# Check for IPs
kubectl get services

# Show worker deployment 
kubectl get deployment/ukb-dask-helm-1-worker

# Resize to zero temporarily
gcloud container clusters resize ukb-dask-1 --node-pool default-pool --num-nodes 0 --zone $GCP_ZONE

# Remove the release (still need to delete kube cluster separately)
helm delete ukb-dask-helm-1

gcloud container clusters delete ukb-dask-1 --zone $GCP_ZONE
```

#### Access

```
# By default, Helm external ips are no longer configured, see
# https://stackoverflow.com/questions/62324275/external-ip-not-exposed-helm-dask.
# For testing, it can be useful to update these with the following:
# helm upgrade ukb-dask-helm-1 dask/dask --set jupyter.serviceType=LoadBalancer --set jupyter.serviceType=LoadBalancer

#######################
# Use Cluster Jupyter #
#######################

export JUPYTER_NOTEBOOK_PORT=8082
export DASK_SCHEDULER_UI_PORT=8081
export DASK_SCHEDULER_PORT=8080
kubectl port-forward --address 0.0.0.0 --namespace default svc/ukb-dask-helm-1-jupyter $JUPYTER_NOTEBOOK_PORT:80 &
kubectl port-forward --address 0.0.0.0 --namespace default svc/ukb-dask-helm-1-scheduler $DASK_SCHEDULER_UI_PORT:80 &
kubectl port-forward --address 0.0.0.0 --namespace default svc/ukb-dask-helm-1-scheduler 8787:80 &
kubectl port-forward --address 0.0.0.0 --namespace default svc/ukb-dask-helm-1-scheduler $DASK_SCHEDULER_PORT:8786 &
# Go to localhost:8082 for jupyter (http://localhost:8082/lab password = dask)

# To work from one extra hop away instead:
# export PROXY_IP="<machine_with_kube_port_forward>"
# ssh -L $JUPYTER_NOTEBOOK_PORT:localhost:$JUPYTER_NOTEBOOK_PORT $PROXY_IP

#####################
# Use Local Jupyter #
#####################

A useful first test:

from dask.distributed import Client
import xarray as xr
import gcsfs
client = Client()
fs = gcsfs.GCSFileSystem()
store = gcsfs.mapping.GCSMap('rs-ukb/prep-data/gt-imputation/ukb_chrXY.zarr', gcs=fs, check=True, create=False)
ds = xr.open_zarr(store)


import zarr
import xarray as xr
import gcsfs
import dask
from dask.diagnostics import ProgressBar
from dask.distributed import Client, config
dask.config.set({"distributed.comm.timeouts.tcp": "50s"})
client = Client()
client
fs = gcsfs.GCSFileSystem()
store = gcsfs.mapping.GCSMap('rs-ukb/prep-data/gt-imputation/ukb_chr22.zarr', gcs=fs, check=True, create=False)
ds = xr.open_zarr(store)
ds
%%time
with ProgressBar():
    cr = ds.call_genotype_probability_mask.mean(dim='samples').compute()
```

## Debug

```bash
# Generate DAG
gcloud auth application-default login
snakemake --dag data/prep-data/gt-imputation/ukb_chrXY.zarr | dot -Tsvg > dag.svg
```

## Development Setup

For local development on this pipeline, run:

```
pip install -r requirements-dev.txt
pre-commit install
```
