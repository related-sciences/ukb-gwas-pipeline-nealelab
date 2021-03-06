# NealeLab 2018 UK Biobank GWAS Reproduction Pipeline

This pipeline is a WIP, but it will attempt to reproduce this [GWAS](http://www.nealelab.is/uk-biobank) (with associated code at [UK_Biobank_GWAS](https://github.com/Nealelab/UK_Biobank_GWAS)) using [sgkit](https://github.com/pystatgen/sgkit).

## Overview

To run this [snakemake](https://snakemake.readthedocs.io/en/stable/) pipeline, the following infrastructure will be utilized at one point or another:

1. A development [GCE](https://cloud.google.com/compute) VM 
    - It is possible for this workstation to exist outside of GCP, but that is not recommended because all clusters configured will not be addressable externally on ports beyond ssh (you will have to add firewall rules and/or modify the cluster installations)
2. [GKE](https://cloud.google.com/kubernetes-engine) clusters
    - These are created for tasks that run arbitrary snakemake jobs but do not need a Dask cluster
3. Dask clusters
    - These will be managed using [Dask Cloud Provider](https://cloudprovider.dask.org/en/latest/)

The development VM should be used to issue snakemake commands and will run some parts of the pipeline locally.  This means that the development VM should have ~24G RAM and ~100G disk space.  It is possible to move these steps on to external GKE clusters, but script execution is faster and easier to debug on a local machine.

## Setup

- Create an `n1-standard-8` GCE instance w/ Debian 10 (buster) OS
- Install NTP (so time is correct after pausing VM):

```bash
sudo apt-get install -y ntp
```
- [Install conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html)
- Initialize the `snakemake` environment, which will provide the CLI from which most other commands will be run:

```bash
conda env create -f envs/snakemake.yaml 
conda activate snakemake
```

Notes: 

- All `gcloud` commands should be issued from this environment (particularly for Kubernetes) since commands are version-sensitive and will often fail if you run commands for a cluster using different `gcloud` versions (i.e. from different environments).
- This will be mentioned frequently in the steps that follow, but it will be assumed when not stated otherwise
that all commands are run from the root of this repo and that the `.env` as well as `env.sh` files have both been sourced.
- Commands will often activated a conda environment first and where not stated otherwise, these environments can be generated using the definitions in [envs](envs).

The `.env` file contains more sensitive variable settings and a prototype for this file is shown here:

```
export GCP_PROJECT=uk-biobank-XXXXX
export GCP_REGION=us-east1
export GCP_ZONE=us-east1-c
export GCS_BUCKET=my-ukb-bucket-name # A single bucket is required for all operations
export GCP_USER_EMAIL=me@company.com # GCP user to be used in ACLs
export UKB_APP_ID=XXXXX      # UK Biobank application id
export GCE_WORK_HOST=ukb-dev # Hostname given to development VM
```

You will have to create this file and populate the variable contents yourself.

## Cluster Management

This pipeline involves steps that require very different resource profiles.  Because of this, 
certain phases of the pipeline will require an appropriately defined GKE or Dask VM cluster.  These 
clusters should be created/modified/deleted when necessary since they can be expensive, and while
the commands below will suggest how to create a cluster, it will be up to the user to ultimately 
decide when they are no longer necessary.  This is not tied into the code because debugging becomes
far more difficult without long-running, user-managed clusters.

### Kubernetes

#### Create Cluster

To create a GKE cluster that snakemake can execute rules on, follow these steps noting
that the parameters used here are illustrative and may need to be altered based on the 
part of the pipeline being run:

```bash
source env.sh; source .env

gcloud init

gcloud components install kubectl

gcloud config set project "$GCP_PROJECT"

# Create cluster with 8 vCPUs/32GiB RAM/200G disk per node
# Memory must be multiple of 256 MiB (argument is MiB)
# Note: increase `--num-nodes` for greater throughput
gcloud container clusters create \
  --machine-type custom-${GKE_IO_NCPU}-${GKE_IO_MEM_MB} \
  --disk-type pd-standard \
  --disk-size ${GKE_IO_DISK_GB}G \
  --num-nodes 1 \
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

#### Modify Cluster

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
gcloud compute instance-groups managed delete-instances gke-ukb-io-1-default-pool-276513bc-grp --instances=gke-ukb-io-1-default-pool-276513bc-48k5 --zone $GCP_ZONE
```

### Dask Cloud Provider

These commands show how to create a Dask cluster either for experimentation or for running steps in this pipeline:

```bash
conda env create -f envs/cloudprovider.yaml 
conda activate cloudprovider

source env.sh; source .env
source config/dask/cloudprovider.sh
python scripts/cluster/cloudprovider.py -- --interactive

>>> create(n_workers=1)
Launching cluster with the following configuration:
  Source Image: projects/ubuntu-os-cloud/global/images/ubuntu-minimal-1804-bionic-v20201014
  Docker Image: daskdev/dask:latest
  Machine Type: n1-standard-8
  Filesytsem Size: 50
  N-GPU Type:
  Zone: us-east1-c
Creating scheduler instance
dask-8a0571b8-scheduler
	Internal IP: 10.142.0.46
	External IP: 35.229.60.113
Waiting for scheduler to run

>>> scale(3)
Creating worker instance
Creating worker instance
dask-9347b93f-worker-60a26daf
	Internal IP: 10.142.0.52
	External IP: 35.229.60.113
dask-9347b93f-worker-4cc3cb6e
	Internal IP: 10.142.0.53
	External IP: 35.231.82.163

>>> adapt(0, 5, interval="60s", wait_count=3)
distributed.deploy.adaptive - INFO - Adaptive scaling started: minimum=0 maximum=5

>>> export_scheduler_info()
Scheduler info exported to /tmp/scheduler-info.txt

>>> shutdown()
Closing Instance: dask-9347b93f-scheduler
Cluster shutdown
```

To see the Dask UI for this cluster, run this on any workstation (outside of GCP):

```gcloud beta compute ssh --zone "us-east1-c" "dask-9347b93f-scheduler" --ssh-flag="-L 8799:localhost:8787"```.

The UI is then available at `http://localhost:8799`.


#### Create Image

A custom image is created in this project as instructed in [Creating custom OS images with Packer](https://cloudprovider.dask.org/en/latest/packer.html#).

The definition of this image is generated automatically based on other environments used in this project, so a new image can be generated by using the following process.

1. Determine package versions to be used by clients and cluster machines.

These can be found by running a command likek this: `docker run daskdev/dask:v2.30.0 conda env export --from-history`.

Alternatively, code with these references is here:

    - https://hub.docker.com/layers/daskdev/dask/2.30.0/images/sha256-fb5d6b4eef7954448c244d0aa7b2405a507f9dad62ae29d9f869e284f0193c53?context=explore
    - https://github.com/dask/dask-docker/blob/99fa808d4dac47b274b5063a23b5f3bbf0d3f105/base/Dockerfile
    
Ensure that the same versions are in [docker/Dockerfile](docker/Dockerfile) as well as [envs/gwas.yaml](envs/gwas.yaml).

2. Create and deploy a new docker image (only necessary if Dask version has changed or new package dependencies were added).

```
DOCKER_USER=<user>
DOCKER_PWD=<password>
DOCKER_TAG="v2020.12.0" # Dask version
cd docker
docker build -t eczech/ukb-gwas-pipeline-nealelab:v2020.12.0 .
echo $DOCKER_PWD | docker login --username $DOCKER_USER --password-stdin
docker push eczech/ukb-gwas-pipeline-nealelab:v2020.12.0
```

**Important**: Update the desired docker image tag in [config/dask/cloudprovider.sh](config/dask/cloudprovider.sh).

3. Build the Packer image 

```
source .env; source env.sh

# From repo root, create the following configuration files:
conda activate cloudprovider
# See https://github.com/dask/dask-cloudprovider/issues/213 for more details (https://gist.github.com/jacobtomlinson/15404d5b032a9f91c9473d1a91e94c0a)
python scripts/cluster/packer.py create_cloud_init_config > config/dask/cloud-init-config.yaml
python scripts/cluster/packer.py create_packer_config > config/dask/packer-config.json

# Run the build
packer build config/dask/packer-config.json
googlecompute: output will be in this color.

==> googlecompute: Checking image does not exist...
==> googlecompute: Creating temporary rsa SSH key for instance...
==> googlecompute: Using image: ubuntu-minimal-1804-bionic-v20201014
==> googlecompute: Creating instance...
    googlecompute: Loading zone: us-east1-c
    googlecompute: Loading machine type: n1-standard-8
    googlecompute: Requesting instance creation...
    googlecompute: Waiting for creation operation to complete...
    googlecompute: Instance has been created!
==> googlecompute: Waiting for the instance to become running...
    googlecompute: IP: 35.196.0.219
==> googlecompute: Using ssh communicator to connect: 35.196.0.219
==> googlecompute: Waiting for SSH to become available...
==> googlecompute: Connected to SSH!
==> googlecompute: Provisioning with shell script: /tmp/packer-shell423808119
    googlecompute: Waiting for cloud-init
    googlecompute: Done
==> googlecompute: Deleting instance...
    googlecompute: Instance has been deleted!
==> googlecompute: Creating image...
==> googlecompute: Deleting disk...
    googlecompute: Disk has been deleted!
Build 'googlecompute' finished after 1 minute 46 seconds.

==> Wait completed after 1 minute 46 seconds

==> Builds finished. The artifacts of successful builds are:
--> googlecompute: A disk image was created: ukb-gwas-pipeline-nealelab-dask-1608465809
```

4. Test the new image.

You can launch an instance of the VM like this:

```
gcloud compute instances create test-image \
  --project $GCP_PROJECT \
  --zone $GCP_ZONE \
  --image-project $GCP_PROJECT \
  --image ukb-gwas-pipeline-nealelab-dask-1608465809
```

This is particularly useful for checking that the GCP monitoring agent was installed correctly.

Then, you can create a Dask cluster to test with like this:

```
source env.sh; source .env; source config/dask/cloudprovider.sh
python scripts/cluster/cloudprovider.py -- --interactive
create(1, machine_type='n1-highmem-2', source_image="ukb-gwas-pipeline-nealelab-dask-1608465809", bootstrap=False)
adapt(0, 5)
export_scheduler_info()

# Compare this to an invocation like this, which would load package dependencies from a file containing
# the environment variables "EXTRA_CONDA_PACKAGES" and "EXTRA_PIP_PACKAGES"
create(1, machine_type='n1-highmem-8', bootstrap=True, env_var_file='config/dask/env_vars.json')
```

Note that a valid `env_var_file` would contain:

```
{
    "EXTRA_CONDA_PACKAGES": "\"numba==0.51.2 xarray==0.16.1 gcsfs==0.7.1 dask-ml==1.7.0 zarr==2.4.0 pyarrow==2.0.0 -c conda-forge\"",
    "EXTRA_PIP_PACKAGES": "\"git+https://github.com/pystatgen/sgkit.git@c5548821653fa2759421668092716d2036834ffe#egg=sgkit\""
}
```

Generally you want to back these dependencies into the docker + GCP vm image, but they can also be introduced by environment
variables like this to aid in development and testing since the image building process is slow.

# Execution

All of the following should be run from the root directory from this repo. 

Note that you can preview the effects of any snakemake command below by adding `-np` to the end. This will show the inputs/outputs to a command as well as any shell code that would be run for it.

```bash
# Run this first before any of the steps below
conda activate snakemake
source env.sh; source .env
```

To get static HTML [performance reports](https://distributed.dask.org/en/latest/diagnosing-performance.html#performance-reports), which are suitable for sharing, do

```bash
mkdir -p logs/reports
export GENERATE_PERFORMANCE_REPORT=True
```

The the reports can be found in `logs/reports`.

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

# Download data dictionary
snakemake --use-conda --cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/pipe-data/external/ukb_meta/data_dictionary_showcase.csv
```

## Zarr Integration


```bash

# Create cluster with enough disk to hold two copies of each bgen file
gcloud container clusters create \
  --machine-type custom-${GKE_IO_NCPU}-${GKE_IO_MEM_MB} \
  --disk-type pd-standard \
  --disk-size ${GKE_IO_DISK_GB}G \
  --num-nodes 1 \
  --enable-autoscaling --min-nodes 1 --max-nodes 9 \
  --zone $GCP_ZONE \
  --node-locations $GCP_ZONE \
  --cluster-version latest \
  --scopes storage-rw \
  $GKE_IO_NAME
  
  
# Run all jobs
# This takes a couple minutes for snakemake to even dry-run, so specifying
# targets yourself is generally faster and more flexible (as shown in the next commands)
snakemake --kubernetes --use-conda --cores=23 --local-cores=1 --restart-times 3 \
--default-remote-provider GS --default-remote-prefix rs-ukb \
--allowed-rules bgen_to_zarr 

# Generate single zarr archive from bgen
# * Set local cores to 1 so that only one rule runs at a time on cluster hosts
snakemake --kubernetes --use-conda --local-cores=1 --restart-times 3 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep/gt-imputation/ukb_chrXY.ckpt
# Expecting running time (8 vCPUs): ~30 minutes

# Scale up to larger files
snakemake --kubernetes --use-conda --cores=2 --local-cores=1 --restart-times 3 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep/gt-imputation/ukb_chr{21,22}.ckpt
# Expecting running time (8 vCPUs): 12 - 14 hours

# Takes ~12 hours for chr 1 on 64 vCPU / 262 GiB RAM / 1TB disk instances.
# Common reasons for failures:
# - https://github.com/dask/gcsfs/issues/315
# - https://github.com/related-sciences/ukb-gwas-pipeline-nealelab/issues/20
gcloud container clusters resize $GKE_IO_NAME --node-pool default-pool --num-nodes 5 --zone $GCP_ZONE
snakemake --kubernetes --use-conda --cores=5 --local-cores=1 --restart-times 3 \
--default-remote-provider GS --default-remote-prefix rs-ukb \
rs-ukb/prep/gt-imputation/ukb_chr{1,2,3,4,5,6,8,9,10}.ckpt

# Run on all chromosomes
snakemake --kubernetes --use-conda --cores=5 --local-cores=1 --restart-times 3 \
--default-remote-provider GS --default-remote-prefix rs-ukb --allowed-rules bgen_to_zarr -np

# Note: With autoscaling, you may will always see one job fail and then get restarted with an error like this
# "Unknown pod snakejob-9174e1f0-c94c-5c76-a3d2-d15af6dd49cb. Has the pod been deleted manually?"

# Delete the cluster
gcloud container clusters delete $GKE_IO_NAME --zone $GCP_ZONE
```


## GWAS QC 


```
# TODO: note somewhere that default quotas of 1000 cpus and 70 IPs will make 62 n1-highmem-16 largest cluster possible

# Create the cluster
screen -S cluster
conda activate cloudprovider
source env.sh; source .env; source config/dask/cloudprovider.sh
python scripts/cluster/cloudprovider.py -- --interactive
create(1, machine_type='n1-highmem-16', source_image="ukb-gwas-pipeline-nealelab-dask-1608465809", bootstrap=False)
adapt(0, 50, interval="60s"); export_scheduler_info(); # Set interval to how long nodes should live between uses

# Run the workflows
screen -S snakemake
conda activate snakemake
source env.sh; source .env  
export DASK_SCHEDULER_IP=`cat /tmp/scheduler-info.txt | grep internal_ip | cut -d'=' -f 2`
export DASK_SCHEDULER_HOST=`cat /tmp/scheduler-info.txt | grep hostname | cut -d'=' -f 2`
export DASK_SCHEDULER_ADDRESS=tcp://$DASK_SCHEDULER_IP:8786
echo $DASK_SCHEDULER_HOST $DASK_SCHEDULER_ADDRESS

# For the UI, open a tunnel by running this command on your local
# workstation before visiting localhost:8799 :
echo "gcloud beta compute ssh --zone us-east1-c $DASK_SCHEDULER_HOST --ssh-flag=\"-L 8799:localhost:8787\""
# e.g. gcloud beta compute ssh --zone us-east1-c dask-6ebe0412-scheduler --ssh-flag="-L 8799:localhost:8787"
    
# Takes ~25 mins for either chr 21/22 on 20 n1-standard-8 nodes.
# Takes ~58 mins for chr 2 on 60 n1-highmem-16 nodes.
snakemake --use-conda --cores=1 --allowed-rules qc_filter_stage_1 --restart-times 3 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep/gt-imputation-qc/ukb_chr{1,2,3,4,5,6,7,8,9,10,13,16}.ckpt
    

# Takes ~25-30 mins for chr 21/22 on 20 n1-standard-8 nodes
# Takes ~52 mins for chr 6 on 60 n1-highmem-16 nodes
snakemake --use-conda --cores=1 --allowed-rules qc_filter_stage_2 --restart-times 3 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/pipe/nealelab-gwas-uni-ancestry-v3/input/gt-imputation/ukb_chr{XY,21,22}.ckpt
    
snakemake --use-conda --cores=1 --allowed-rules qc_filter_stage_1 --restart-times 3 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep/gt-imputation-qc/ukb_chr{11,12,13,14,15,16,17,18,19,20}.ckpt
```

## Phenotype Prep

These steps can be run locally, but the local machine must be resized
to have at least 200G RAM.  They can alternatively be run on a GKE
cluster by adding `--kubernetes` to the commands below.

```bash
conda activate snakemake
source env.sh; source .env;

# Create the input PHESANT phenotype CSV (takes ~15 mins)
snakemake --use-conda --cores=1 --allowed-rules main_csv_phesant_field_prep \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep-data/main/ukb_phesant_prep.csv
    
# Extract sample ids from genetic data QC (~1 minute)
snakemake --use-conda --cores=1 --allowed-rules extract_gwas_qc_sample_ids \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/pipe/nealelab-gwas-uni-ancestry-v3/input/sample_ids.csv

# Clone PHESANT repository to download normalization script and metadata files
snakemake --use-conda --cores=1 --allowed-rules phesant_clone \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/temp/repos/PHESANT

# Use genetic data QC sample ids as filter on samples used in phenotype preparation (takes ~40 mins, uses 120G RAM)
snakemake --use-conda --cores=1 --allowed-rules filter_phesant_csv \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep/main/ukb_phesant_filtered.csv
    
# Generate the normalized phenotype data (took 8 hrs and 24 minutes on 8 vCPU / 300 GB RAM)
snakemake --use-conda --cores=1 --allowed-rules main_csv_phesant_phenotypes \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep/main/ukb_phesant_phenotypes.csv > phesant.log 2>&1
    
# This isn't stricly necessary, but these logs should be preserved for future debugging
gsutil cp /tmp/phesant/phenotypes.1.log gs://rs-ukb/prep/main/log/phesant/phenotypes.1.log
gsutil cp phesant.log gs://rs-ukb/prep/main/log/phesant/phesant.log

# Dump the resulting field ids into a separate csv for debugging
snakemake --use-conda --cores=1 --allowed-rules main_csv_phesant_phenotypes_field_id_export \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep/main/ukb_phesant_phenotypes.field_ids.csv
    
# Convert the phenotype data to parquet (~45 mins)
snakemake --use-conda --cores=1 --allowed-rules convert_phesant_csv_to_parquet \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep/main/ukb_phesant_phenotypes.parquet.ckpt
    
# Convert the phenotype data to zarr (~30 mins)
snakemake --use-conda --cores=1 --allowed-rules convert_phesant_parquet_to_zarr \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep/main/ukb_phesant_phenotypes.zarr.ckpt
    
# Sort the zarr according to the sample ids in imputed genotyping data (~45 mins)
snakemake --use-conda --cores=1 --allowed-rules sort_phesant_parquet_zarr \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/pipe/nealelab-gwas-uni-ancestry-v3/input/main/ukb_phesant_phenotypes.ckpt
    
```

## GWAS 

```
# Notes: 
# - Client machine for these steps can be minimal (4 vCPU, 16 GB RAM)
# - A dask cluster should be created first as it was in the GWAS QC steps

# Copy Neale Lab sumstats from Open Targets
snakemake --use-conda --cores=1 --allowed-rules import_ot_nealelab_sumstats \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/external/ot_nealelab_sumstats/copy.ckpt
    
# Generate list of traits for GWAS based on intersection of 
# PHESANT results and OT sumstats
snakemake --use-conda --cores=1 --allowed-rules trait_group_ids \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/pipe/nealelab-gwas-uni-ancestry-v3/input/trait_group_ids.csv
    
# Generate sumstats using sgkit    
# See https://github.com/pystatgen/sgkit/issues/390 for timing information on this step.
snakemake --use-conda --cores=1 --allowed-rules gwas --restart-times 3 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/pipe/nealelab-gwas-uni-ancestry-v3/output/gt-imputation/ukb_chr{21,22}.ckpt

# To clear: gsutil -m rm -rf gs://rs-ukb/pipe/nealelab-gwas-uni-ancestry-v3/output/gt-imputation/{sumstats,variables,*.ckpt}

# Takes ~10 mins on local host
snakemake --use-conda --cores=1 --allowed-rules sumstats \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/pipe/nealelab-gwas-uni-ancestry-v3/output/sumstats-1990-20095.parquet
    
```

## Misc

- Never let fsspec overwrite Zarr archives!  This technically works but it is incredibly slow compared to running "gsutil -m rm -rf <path>" yourself.  Another way to phrase this is that if you are expecting a pipeline step to overwrite an existing Zarr archive, delete it manually first.
- To run the snakemake container manually, e.g. if you want to debug a GKE job, run `docker run --rm -it -v $HOME/repos/ukb-gwas-pipeline-nealelab:/tmp/ukb-gwas-pipeline-nealelab snakemake/snakemake:v5.30.1 /bin/bash`
    - This version should match that of the snakemake version used in the `snakemake.yaml` environment

## Debug

```bash
# Generate DAG
gcloud auth application-default login
snakemake --dag data/prep-data/gt-imputation/ukb_chrXY.zarr | dot -Tsvg > dag.svg
```

#### Traits

This is a list of UKB traits that can be useful for testing or better understanding data coding schemes and PHESANT phenotype generation (or that are just entertaining):

- [5610](https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=5610) - Which eye(s) affected by presbyopia (categorical)
- [50](https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=50) - Standing height (continuous)
- [5183](https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=5183) - Current eye infection (binary)
- [20101](https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=20101) - Thickness of butter/margarine spread on bread rolls (categorical)
- [23098](https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=23098) - Weight (continuous)
- [2395](https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=2395) - Hair/balding pattern (categorical)
- [1990](https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=1990) - Tense / 'highly strung' (binary)
- [20095](https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=20095) - Size of white wine glass drunk (categorical)
- [845](https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=845) - Age completed full time education (continuous)
- [1160](https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=1160) - Sleep duration (continuous)
- [4041](https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=4041) - Gestational diabetes only (binary)
- [738](https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=738) - Average total household income before tax (categorical)
- [1100](https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=1100) - Drive faster than motorway speed limit (categorical)
- [1100](https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=1150) - Usual side of head for mobile phone use (categorical)


## Development Setup

For local development on this pipeline, run:

```
pip install -r requirements-dev.txt
pre-commit install
```

