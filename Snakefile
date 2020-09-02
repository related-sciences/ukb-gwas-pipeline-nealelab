import pandas as pd
import os
import os.path as osp
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

def to_df(contigs):
    return pd.DataFrame(contigs).astype(str).set_index('name', drop=False)

plink_contigs = to_df(config['raw']['plink']['contigs'])
bgen_contigs = to_df(config['raw']['bgen']['contigs'])

rule all:
    input:
        expand(
            "prep-data/gt-calls/ukb_chr{plink_contig}.ckpt", 
            plink_contig=plink_contigs['name']
        ),
        expand(
            "prep-data/gt-imputation/ukb_chr{bgen_contig}.ckpt", 
            bgen_contig=bgen_contigs['name']
        ),
        "prep-data/main/ukb.ckpt",
        "pipe-data/external/ukb_meta/data_dictionary_showcase.csv",
        "prep-data/main/ukb_sample_qc.csv",
        "prep-data/main/ukb_sample_qc.ckpt"


rule plink_to_zarr:
    input:
        bed_path="raw-data/gt-calls/ukb_cal_chr{plink_contig}_v2.bed",
        bim_path="raw-data/gt-calls/ukb_snp_chr{plink_contig}_v2.bim",
        fam_path="raw-data/gt-calls/ukb59384_cal_chr{plink_contig}_v2_s488264.fam"
    output:
        "prep-data/gt-calls/ukb_chr{plink_contig}.ckpt"
    threads: config['env']['gke_io_ncpu'] - 1
    conda:
        "envs/io.yaml"
    params:
        zarr_path=lambda wc: bucket_path(f"prep-data/gt-calls/ukb_chr{wc.plink_contig}.zarr"),
        contig_index=lambda wc: plink_contigs.loc[str(wc.plink_contig)]['index']
    shell:
        "python scripts/convert_genetic_data.py plink_to_zarr "
        "--input-path-bed={input.bed_path} "
        "--input-path-bim={input.bim_path} "
        "--input-path-fam={input.fam_path} "
        "--output-path={params.zarr_path} "
        "--contig-name={wildcards.plink_contig} "
        "--contig-index={params.contig_index} "
        "--remote=True "
        "&& touch {output}"


def bgen_samples_path(wc):
    n_samples = bgen_contigs.loc[wc.bgen_contig]['n_consent_samples']
    return [f"raw-data/gt-imputation/ukb59384_imp_chr{wc.bgen_contig}_v3_s{n_samples}.sample"]

# Takes ~14 hr on 8 cores for chr{21,22}
rule bgen_to_zarr:
    input: # TODO: Don't rerun until deciding on https://github.com/related-sciences/ukb-gwas-pipeline-nealelab/issues/5
        bgen_path="raw-data/gt-imputation/ukb_imp_chr{bgen_contig}_v3.bgen",
        variants_path="raw-data/gt-imputation/ukb_mfi_chr{bgen_contig}_v3.txt",
        samples_path=bgen_samples_path
    output:
        "prep-data/gt-imputation/ukb_chr{bgen_contig}.ckpt"
    threads: config['env']['gke_io_ncpu'] - 1
    resources:
        mem_mb=lambda wc: (config['env']['gke_io_ncpu'] - 1) * 4000
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
rule main_csv_to_parquet:
    input:
        f"raw-data/main/ukb{ukb_app_id}.csv"
    output:
        "prep-data/main/ukb.ckpt"
    params:
        parquet_path=bucket_path("prep-data/main/ukb.parquet")
    conda:
        "envs/spark.yaml"
    shell:
        "export SPARK_DRIVER_MEMORY=12g && "
        "python scripts/convert_main_data.py csv_to_parquet "
        "--input-path={input} "
        "--output-path={params.parquet_path} && "
        "gsutil -m -q rsync -d -r {params.parquet_path} gs://{params.parquet_path} && "
        "touch {output}"
        
        
rule download_data_dictionary:
    input:
        HTTP.remote("biobank.ctsu.ox.ac.uk/~bbdatan/Data_Dictionary_Showcase.csv")
    output:
        "pipe-data/external/ukb_meta/data_dictionary_showcase.csv"
    shell:
        "mv {input} {output}"
        
rule download_efo_mapping:
    input:
        HTTP.remote("https://raw.githubusercontent.com/EBISPOT/EFO-UKB-mappings/6e055ee03dd3c36ed62c1b3c41ac7a50f4864b30/UK_Biobank_master_file.tsv")
    output:
        "pipe-data/external/ukb_meta/efo_mapping.csv"
    shell:
        "mv {input} {output}"
        
rule import_otg_v2d_json:
    output:
        "pipe-data/external/otg/20.02.01/v2d.json.ckpt"
    params:
        input_path="open-targets-genetics-releases/20.02.01/v2d",
        output_path=bucket_path("pipe-data/external/otg/20.02.01/v2d.json")
    shell:
        # Requester pays bucket requires user project for billing
        "gsutil -u {gcp_project} -m rsync -r gs://{params.input_path} gs://{params.output_path} && touch {output}"
        
# rule convert_otg_v2d_to_parquet:
#     input: rules.import_otg_v2d_json.output
#     output:
#         "pipe-data/external/otg/20.02.01/v2d.parquet.ckpt"
#     conda: 
#         "envs/spark.yaml"
#     run:
#         from pyspark.sql import SparkSession
#         spark = SparkSession.builder.getOrCreate()
#         df = spark.read.json(input[0])
#         df = df.repartition(18)
#         df.write.parquet(output[0])
        

rule extract_sample_qc_csv:
    input: rules.main_csv_to_parquet.output
    output: 
        "prep-data/main/ukb_sample_qc.csv"
    params:
        input_path=bucket_path("prep-data/main/ukb.parquet")
    conda:
        "envs/spark.yaml"
    shell:
        "export SPARK_DRIVER_MEMORY=10g && "
        "python scripts/extract_main_data.py sample_qc_csv "
        "--input-path={params.input_path} "
        "--output-path={output} "
        
rule extract_sample_qc_zarr:
    input: rules.extract_sample_qc_csv.output
    output: 
        "prep-data/main/ukb_sample_qc.ckpt"
    params:
        output_path=bucket_path("prep-data/main/ukb_sample_qc.zarr")
    conda:
        "envs/gwas.yaml"
    shell:
        "python scripts/extract_main_data.py sample_qc_zarr "
        "--input-path={input} "
        "--output-path={params.output_path} "
        "--remote=True && "
        "touch {output}"

rule extract_sample_sets:
    input:
        # This was downloaded manually from the NealeLab results spreadsheet
        # before all the Dropbox links broke
        # See: https://docs.google.com/spreadsheets/d/1kvPoupSzsSFBNSztMzl04xMoSC3Kcx3CrjVf4yBmESU/edit?ts=5b5f17db#gid=178908679
        # TODO: Update to download as separate step when it's possible to get these files
        european_samples="pipe-data/external/nealelab_v3_20180731/european_samples.tsv"
    output:
        # pipe-data/nealelab_rapid_gwas/import
        # pipe-data/nealelab_rapid_gwas/run/202008
        "pipe-data/external/nealelab_v3_20180731/extract/sample_sets.csv"
    conda:
        "envs/spark.yaml"
    shell:
        "export SPARK_DRIVER_MEMORY=10g && "
        "python scripts/extract_external_data.py nlv3_sample_sets "
        "--input-path-european-samples={input.european_samples} "
        "--output-path={output}"
        
        
onsuccess:
    print("Workflow finished successfully")

onerror:
    print("Workflow failed")