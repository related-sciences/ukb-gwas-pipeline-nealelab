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
source .env

gcloud init

gcloud components install kubectl

gcloud config set project "$GOOGLE_CLOUD_PROJECT"

# Create cluster with 2 vCPUs/12G RAM/100G disk per node
gcloud container clusters create \
  --machine-type custom-2-12288 \
  --disk-type pd-standard \
  --disk-size 100G \
  --num-nodes 1 \
  --zone $GCP_ZONE \
  --node-locations $GCP_ZONE \
  --cluster-version latest \
  --scopes storage-rw \
  $CLUSTER_IO

gcloud container clusters get-credentials ukb-io --zone $GCP_ZONE

# Grant admin permissions on cluster
kubectl create clusterrolebinding cluster-admin-binding \
  --clusterrole=cluster-admin \
  --user=$GOOGLE_EMAIL_ACCOUNT
  
# If you see this, add IAM policy as below
Error from server (Forbidden): clusterrolebindings.rbac.authorization.k8s.io is forbidden: User "XXXXX" cannot create resource "clusterrolebindings" in API group "rbac.authorization.k8s.io" at the cluster scope: requires one of ["container.clusterRoleBindings.create"] permission(s).

gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member=user:$GOOGLE_EMAIL_ACCOUNT \
  --role=roles/container.admin

# Login necessary for GS Read/Write
gcloud auth application-default login

# Run the workflow
snakemake --kubernetes --use-conda \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    -np rs-ukb/prep-data/gt-imputation/ukb_chrXY.zarr
snakemake --kubernetes --use-conda \
    --default-remote-provider GS --default-remote-prefix rs-ukb \
    rs-ukb/prep-data/gt-imputation/ukb_chrXY.zarr
    
# Check on the cluster
kubectl get node # Find node name
gcloud compute ssh gke-ukb-io-default-pool-XXXXX

# Remove the cluster
gcloud container clusters delete ukb-io
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