# ukb-gwas-pipeline-nealelab

Pipeline for reproduction of NealeLab 2018 UKB GWAS

## Setup

```bash
conda env create -f envs/snakemake.yml 
conda activate snakemake
```

## Execution

Local (out of GCP):

```bash
export GOOGLE_CLOUD_PROJECT="UK Biobank"
gcloud auth application-default login

snakemake --default-remote-provider=GS --default-remote-prefix=rs-ukb -np \
    rs-ukb/prep-data/gt-imputation/ukb_chrXY.zarr

snakemake --default-remote-provider=GS --default-remote-prefix=rs-ukb --dag \
    rs-ukb/prep-data/gt-imputation/ukb_chrXY.zarr
snakemake --dag data/prep-data/gt-imputation/ukb_chrXY.zarr | dot -Tsvg > dag.svg
```
