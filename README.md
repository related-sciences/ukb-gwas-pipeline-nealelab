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
source env.sh

gcloud init

gcloud components install kubectl

gcloud config set project "$GCP_PROJECT"

# Create cluster with 8 vCPUs/24GiB RAM/200G disk per node (argument is MB)
# Memory must be multiple of 256 MiB
gcloud container clusters create \
  --machine-type custom-${GKE_IO_NCPU}-24576 \
  --disk-type pd-standard \
  --disk-size 200G \
  --num-nodes 1 \
  --zone $GCP_ZONE \
  --node-locations $GCP_ZONE \
  --cluster-version latest \
  --scopes storage-rw \
  $GKE_IO_NAME

gcloud container clusters get-credentials $GKE_CLUSTER_IO --zone $GCP_ZONE

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

# Resize cluster and run on more files:
gcloud container clusters resize $GKE_CLUSTER_IO --node-pool default-pool --num-nodes 2 --zone $GCP_ZONE
snakemake --kubernetes --use-conda --cores=2 --local-cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep-data/gt-imputation/ukb_chr{21,22}.ckpt
# Worker running times:
# 1: 12h 12m
# 2: 14h 50m


gcloud container clusters resize $GKE_CLUSTER_IO --node-pool default-pool --num-nodes 1 --zone $GCP_ZONE

    
snakemake --kubernetes --use-conda --cores=1 --local-cores=1 \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep-data/main/ukb.ckpt
    
    
# Check on the cluster
kubectl get node # Find node name
gcloud compute ssh gke-ukb-io-default-pool-XXXXX

# Remove the cluster
gcloud container clusters delete $GKE_CLUSTER_IO
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