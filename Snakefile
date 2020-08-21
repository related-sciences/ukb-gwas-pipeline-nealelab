import pandas as pd
import os
import os.path as osp
from snakemake.remote.GS import RemoteProvider as GSRemoteProvider
GS = GSRemoteProvider()

configfile: "config.yaml"

def to_df(contigs):
    return pd.DataFrame(contigs).astype(str).set_index('name', drop=False)
plink_contigs = to_df(config['raw']['plink']['contigs'])
bgen_contigs = to_df(config['raw']['bgen']['contigs'])
# bucket = os.environ['GCS_BUCKET']

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


rule bgen_to_zarr:
    input: # TODO: Don't rerun until deciding on https://github.com/related-sciences/ukb-gwas-pipeline-nealelab/issues/5
        bgen_path="raw-data/gt-imputation/ukb_imp_chr{bgen_contig}_v3.bgen",
        variants_path="raw-data/gt-imputation/ukb_mfi_chr{bgen_contig}_v3.txt",
        samples_path=bgen_samples_path
    output:
        "prep-data/gt-imputation/ukb_chr{bgen_contig}.ckpt"
    threads: 6 # TODO: how can this be all available cores?
    params:
        zarr_path=lambda wc: f"rs-ukb/prep-data/gt-imputation/ukb_chr{wc.bgen_contig}.zarr", # TODO: use bucket name variable
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
        #"raw-data/main/ukb41430.csv"
        "prep-data/main/ukb.1k.csv"
    output:
        directory("prep-data/main/ukb.1k.parquet")
    threads: 7
#     params:
#         parquet_path=lambda wcc: f"rs-ukb/prep-data/main/ukb.parquet"
    conda:
        "envs/spark.yaml"
    shell:
        "export JAVA_HOME=$CONDA_PREFIX/jre && "
        "export SPARK_DRIVER_MEMORY=12g && "
        "python scripts/convert_main_data.py csv_to_parquet "
        "--input-path={input} "
        "--output-path={output} "
        
        
onsuccess:
    print("Workflow finished, no error")

onerror:
    print("An error occurred")