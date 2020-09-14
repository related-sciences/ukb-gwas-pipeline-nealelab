from snakemake.remote.GS import RemoteProvider as GSRemoteProvider
from snakemake.remote.HTTP import RemoteProvider as HTTPRemoteProvider

HTTP = HTTPRemoteProvider()
GS = GSRemoteProvider()

configfile: "config.yaml"

bucket = os.getenv('GCS_BUCKET', config['env']['gcs_bucket'])
ukb_app_id = os.getenv('UKB_APP_ID', config['env']['ukb_app_id'])
gcp_project = os.getenv('GCP_PROJECT', config['env']['gcp_project'])

def bucket_path(path):
    return bucket + '/' + path

include: "rules/zarr_integration.smk"
include: "rules/primary_integration.smk"
include: "rules/phenotype_prep.smk"
        
onsuccess:
    print("Workflow finished successfully")

onerror:
    print("Workflow failed")
    