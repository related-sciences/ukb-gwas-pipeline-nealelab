import pandas as pd
import os
import os.path as osp
from snakemake.remote.GS import RemoteProvider as GSRemoteProvider
GS = GSRemoteProvider()

configfile: "config.yaml"

plink_contigs = pd.DataFrame(config['raw']['plink']['contigs']).set_index('name', drop=False)
bgen_contigs = pd.DataFrame(config['raw']['bgen']['contigs']).set_index('name', drop=False)

bucket = os.environ['GCS_BUCKET']

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
        GS.remote(bucket + "/prep-data/gt-calls/ukb_chr{plink_contig}.zarr", stay_on_remote=True)
    conda:
        "envs/gwas.yaml"
    log:
        "logs/plink_to_zarr.{plink_contig}.txt"
    params:
        contig_index=lambda wc: plink_contigs.loc[wc.plink_contig]['index']
    shell:
        "python scripts/convert.py plink_to_zarr "
        "--input-path-bed={input.bed_path} "
        "--input-path-bim={input.bim_path} "
        "--input-path-fam={input.fam_path} "
        "--output-path={output} "
        "--contig-name={wildcards.plink_contig} "
        "--contig-index={params.contig_index} "
        "--remote=True "
        "2> {log}"


def bgen_samples_path(wc):
    n_samples = bgen_contigs.loc[wc.bgen_contig]['n_consent_samples']
    return [f"raw-data/gt-imputation/ukb59384_imp_chr{wc.bgen_contig}_v3_s{n_samples}.sample"]


rule bgen_to_zarr:
    input:
        bgen_path="raw-data/gt-imputation/ukb_imp_chr{bgen_contig}_v3.bgen",
        variants_path="raw-data/gt-imputation/ukb_mfi_chr{bgen_contig}_v3.txt",
        samples_path=bgen_samples_path
    output:
        #GS.remote("prep-data/gt-imputation/ukb_chr{bgen_contig}.zarr", stay_on_remote=True)
        "prep-data/gt-imputation/ukb_chr{bgen_contig}.zarr"
    params:
        contig_index=lambda wc: bgen_contigs.loc[wc.bgen_contig]['index']
    conda:
        "envs/gwas.yaml"
    log:
        "logs/bgen_to_zarr.{bgen_contig}.txt"
    shell:
        "python scripts/convert.py bgen_to_zarr "
        "--input-path-bgen={input.bed_path} "
        "--input-path-variants={input.variants_path} "
        "--input-path-samples={input.samples_path} "
        "--output-path={output} "
        "--contig-name={wildcards.bgen_contig} "
        "--contig-index={params.contig_index} "
        "--remote=True "
        "2> {log}"
        
onsuccess:
    print("Workflow finished, no error")

onerror:
    print("An error occurred")