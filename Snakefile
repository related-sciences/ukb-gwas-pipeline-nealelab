from snakemake.remote.GS import RemoteProvider as GSRemoteProvider
from snakemake.remote.HTTP import RemoteProvider as HTTPRemoteProvider

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

def bucket_path(path):
    return bucket + '/' + path

include: "rules/zarr_integration.smk"
include: "rules/primary_integration.smk"
include: "rules/phenotype_prep.smk"
        
onsuccess:
    print("Workflow finished successfully")

onerror:
    print("Workflow failed")
    