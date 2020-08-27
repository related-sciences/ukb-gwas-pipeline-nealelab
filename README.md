# ukb-gwas-pipeline-nealelab

Pipeline for reproduction of NealeLab 2018 UKB GWAS

## Setup

```bash
conda env create -f envs/snakemake.yaml 
conda activate snakemake
```

## Execution

See:

- https://snakemake.readthedocs.io/en/stable/executing/cloud.html
- https://zero-to-jupyterhub.readthedocs.io/en/latest/google/step-zero-gcp.html

```bash
conda activate snakemake
source env.sh

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

gcloud container clusters get-credentials $GKE_IO_NAME --zone $GCP_ZONE

# Grant admin permissions on cluster
kubectl create clusterrolebinding cluster-admin-binding \
  --clusterrole=cluster-admin \
  --user=$GCP_USER_EMAIL
  
# If you see this, add IAM policy as below
Error from server (Forbidden): clusterrolebindings.rbac.authorization.k8s.io is forbidden: User "XXXXX" cannot create resource "clusterrolebindings" in API group "rbac.authorization.k8s.io" at the cluster scope: requires one of ["container.clusterRoleBindings.create"] permission(s).

gcloud projects add-iam-policy-binding $GCP_PROJECT \
  --member=user:$GCP_USER_EMAIL \
  --role=roles/container.admin

# Login necessary for GS Read/Write
gcloud auth application-default login

# Dryrun for workflow
snakemake --kubernetes --use-conda --local-cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    -np rs-ukb/prep-data/gt-imputation/ukb_chrXY.ckpt
    
# Set local cores to 1 so that only one rule runs at a time on cluster hosts
snakemake --kubernetes --use-conda --local-cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep-data/gt-imputation/ukb_chrXY.ckpt
# Expecting running time on 8vCPU/24G: ~40 minutes

# Resize cluster and run on more files:
gcloud container clusters resize $GKE_IO_NAME --node-pool default-pool --num-nodes 2 --zone $GCP_ZONE

snakemake --kubernetes --use-conda --cores=2 --local-cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep-data/gt-imputation/ukb_chr{21,22}.ckpt
# Worker running times:
# 1: 12h 12m
# 2: 14h 50m


gcloud container clusters resize $GKE_CLUSTER_IO --node-pool default-pool --num-nodes 1 --zone $GCP_ZONE

# Convert main dataset to parquet
snakemake --use-conda --cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep-data/main/ukb.ckpt
    
# Extract sample QC from main dataset (as csv)
snakemake --use-conda --cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep-data/main/ukb_sample_qc.csv
    
# Extract sample sets
snakemake --use-conda --cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/pipe-data/external/nealelab_v3_20180731/extract/sample_sets.csv

# Download data dictionary
snakemake --use-conda --cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/pipe-data/external/ukb_meta/data_dictionary_showcase.csv
    
    
# Import OTG V2D
snakemake --use-conda --cores=1 -np \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/pipe-data/external/otg/20.02.01/v2d.json.ckpt \
    rs-ukb/pipe-data/external/otg/20.02.01/v2d.parquet.ckpt
    
# Download EFO mapping for OTG
snakemake --use-conda --cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/pipe-data/external/ukb_meta/efo_mapping.csv
    
# Check on the cluster
kubectl get node # Find node name
gcloud compute ssh gke-ukb-io-default-pool-XXXXX

# Remove the cluster
gcloud container clusters delete $GKE_IO_NAME --zone $GCP_ZONE

# Remove node from running cluster
# https://pminkov.github.io/blog/removing-a-node-from-a-kubernetes-cluster-on-gke-google-container-engine.html
kubectl get nodes
# Find node to delete: gke-ukb-io-1-default-pool-276513bc-48k5
kubectl drain gke-ukb-io-1-default-pool-276513bc-48k5 --force --ignore-daemonsets
gcloud container clusters describe ukb-io-1 --zone us-east1-c 
# Find instance group name: gke-ukb-io-1-default-pool-276513bc-grp
gcloud compute instance-groups managed delete-instances gke-ukb-io-1-default-pool-276513bc-grp --instances=gke-ukb-io-1-default-pool-276513bc-48k5 --zone us-east1-c 

```

## Debug

Local (out of GCP):

```bash
gcloud auth application-default login

snakemake --default-remote-provider=GS --default-remote-prefix=rs-ukb -np \
    rs-ukb/prep-data/gt-imputation/ukb_chrXY.zarr

snakemake --default-remote-provider=GS --default-remote-prefix=rs-ukb --dag \
    rs-ukb/prep-data/gt-imputation/ukb_chrXY.zarr
snakemake --dag data/prep-data/gt-imputation/ukb_chrXY.zarr | dot -Tsvg > dag.svg
```