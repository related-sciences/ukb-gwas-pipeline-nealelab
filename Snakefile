from snakemake.remote.GS import RemoteProvider as GSRemoteProvider
from snakemake.remote.HTTP import RemoteProvider as HTTPRemoteProvider
import pandas as pd

HTTP = HTTPRemoteProvider()
GS = GSRemoteProvider()

configfile: "config.yaml"

envvars:
    "UKB_APP_ID",
    "GCS_BUCKET",
    "GCP_PROJECT",
    "GKE_IO_NCPU",
    "GKE_IO_MEM_MB"

bucket = os.environ['GCS_BUCKET']
ukb_app_id = os.environ['UKB_APP_ID']
gcp_project = os.environ['GCP_PROJECT']
gke_io_ncpu = int(os.environ['GKE_IO_NCPU'])
gke_io_mem_mb = int(os.environ['GKE_IO_MEM_MB'])
gke_io_mem_req_mb = int(.85 * gke_io_mem_mb)


def bucket_path(path, add_protocol=False):
    path = bucket + '/' + path
    if add_protocol:
        path = 'gs://' + path
    return path

def to_df(contigs):
    return pd.DataFrame(contigs).astype(str).set_index('name', drop=False)

plink_contigs = to_df(config['raw']['plink']['contigs'])
bgen_contigs = to_df(config['raw']['bgen']['contigs'])

rule all:
    input:
        expand(
            "prep/gt-imputation/ukb_chr{bgen_contig}.ckpt", 
            bgen_contig=bgen_contigs['name']
        ),
        # "prep/main/ukb.ckpt",
        # "prep/main/meta/data_dictionary_showcase.csv",
        # "prep/main/ukb_sample_qc.csv",
        "prep/main/ukb_sample_qc.ckpt",
        expand(
            "pipe/nealelab-gwas-uni-ancestry-v3/output/gt-imputation/ukb_chr{bgen_contig}.ckpt", 
            bgen_contig=bgen_contigs['name']
        ),
        "prep/main/ukb_phesant_phenotypes-subset01.csv",
        "pipe/nealelab-gwas-uni-ancestry-v3/output/sumstats.parquet"


include: "rules/zarr_integration.smk"
include: "rules/primary_integration.smk"
include: "rules/sumstat_integration.smk"
include: "rules/phenotype_integration.smk"
include: "rules/gwas_pipeline.smk"
        
onsuccess:
    print("Workflow finished successfully")

onerror:
    print("Workflow failed")
    