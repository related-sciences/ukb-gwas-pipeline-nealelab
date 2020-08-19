import pandas as pd
import os
import os.path as osp
from snakemake.remote.GS import RemoteProvider as GSRemoteProvider
GS = GSRemoteProvider()

configfile: "config.yaml"

bucket = os.getenv('GCS_BUCKET', config['gcs_bucket'])
ukb_app_id = os.getenv('UKB_APP_ID', config['ukb_app_id'])

def bucket_path(path):
    return bucket + '/' + path

def to_df(contigs):
    return pd.DataFrame(contigs).astype(str).set_index('name', drop=False)

plink_contigs = to_df(config['raw']['plink']['contigs'])
bgen_contigs = to_df(config['raw']['bgen']['contigs'])

rule all:
    input:
        expand(
            "prep-data/gt-calls/ukb_chr{plink_contig}.zarr", 
            plink_contig=plink_contigs['name']
        ),
        expand(
            "prep-data/gt-imputation/ukb_chr{bgen_contig}.zarr", 
            bgen_contig=bgen_contigs['name']
        )


rule plink_to_zarr:
    input:
        bed_path="raw-data/gt-calls/ukb_cal_chr{plink_contig}_v2.bed",
        bim_path="raw-data/gt-calls/ukb_snp_chr{plink_contig}_v2.bim",
        fam_path="raw-data/gt-calls/ukb59384_cal_chr{plink_contig}_v2_s488264.fam"
    output:
        directory("prep-data/gt-calls/ukb_chr{plink_contig}.zarr")
    conda:
        "envs/io.yaml"
    log:
        "logs/plink_to_zarr.{plink_contig}.txt"
    params:
        contig_index=lambda wc: plink_contigs.loc[str(wc.plink_contig)]['index']
    shell:
        "python scripts/convert_genetic_data.py plink_to_zarr "
        "--input-path-bed={input.bed_path} "
        "--input-path-bim={input.bim_path} "
        "--input-path-fam={input.fam_path} "
        "--output-path={output} "
        "--contig-name={wildcards.plink_contig} "
        "--contig-index={params.contig_index} "
        "--remote=False "
        "2> {log}"


def bgen_samples_path(wc):
    n_samples = bgen_contigs.loc[wc.bgen_contig]['n_consent_samples']
    return [f"raw-data/gt-imputation/ukb59384_imp_chr{wc.bgen_contig}_v3_s{n_samples}.sample"]

# Takes ~12 hr on 8 cores for chr{21,22}
rule bgen_to_zarr:
    input:
        bgen_path="raw-data/gt-imputation/ukb_imp_chr{bgen_contig}_v3.bgen",
        variants_path="raw-data/gt-imputation/ukb_mfi_chr{bgen_contig}_v3.txt",
        samples_path=bgen_samples_path
    output:
        "prep-data/gt-imputation/ukb_chr{bgen_contig}.ckpt"
    threads: config['gke_io_ncpu'] - 1
    params:
        zarr_path=lambda wc: bucket_path(f"prep-data/gt-imputation/ukb_chr{wc.bgen_contig}.zarr"),
        contig_index=lambda wc: bgen_contigs.loc[str(wc.bgen_contig)]['index']
    conda:
        "envs/io.yaml"
    shell:
        "python scripts/convert_genetic_data.py bgen_to_zarr "
        "--input-path-bgen={input.bgen_path} "
        "--input-path-variants={input.variants_path} "
        "--input-path-samples={input.samples_path} "
        "--output-path={params.zarr_path} "
        "--contig-name={wildcards.bgen_contig} "
        "--contig-index={params.contig_index} "
        "--remote=True "
        "&& touch {output}"
        

# Takes ~45 mins on 4 cores, 12g heap
rule csv_to_parquet:
    input:
        f"raw-data/main/ukb{ukb_app_id}.csv"
    output:
        "prep-data/main/ukb.ckpt"
    params:
        parquet_path=lambda wcc: bucket_path("prep-data/main/ukb.parquet")
    conda:
        "envs/spark.yaml"
    shell:
        "export JAVA_HOME=$CONDA_PREFIX/jre && "
        "export SPARK_DRIVER_MEMORY=12g && "
        "conda list && "
        "python scripts/convert_main_data.py csv_to_parquet "
        "--input-path={input} "
        "--output-path={params.parquet_path} && "
        "gsutil -m -q rsync -d -r {params.parquet_path} gs://{params.parquet_path} && "
        "touch {output}"
        
onsuccess:
    print("Workflow finished, no error")

onerror:
    print("An error occurred")