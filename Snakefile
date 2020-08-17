import pandas as pd
import os
import os.path as osp
from snakemake.remote.GS import RemoteProvider as GSRemoteProvider
GS = GSRemoteProvider()

configfile: "config.yaml"

plink_contigs = pd.DataFrame(config['raw']['plink']['contigs']).set_index('name', drop=False)
bgen_contigs = pd.DataFrame(config['raw']['bgen']['contigs']).set_index('name', drop=False)

cache_dir = os.getenv('CACHE_DIR', 'data/cache')

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
        "prep-data/gt-calls/ukb_chr{plink_contig}.zarr"
    conda:
        "envs/gwas.yaml"
    log:
        "logs/plink_to_zarr.{plink_contig}.txt"
    params:
        contig_index=lambda wc: plink_contigs.loc[wc.plink_contig]['index']
    shell:
        "python scripts/plink_to_zarr.py run "
        "--bed-path={input.bed_path} "
        "--bim-path={input.bim_path} "
        "--fam-path={input.fam_path} "
        "--output-path={output} "
        "--contig-name={wildcards.plink_contig} "
        "--contig-index={params.contig_index}"

rule bgen_to_zarr:
    input:
        "raw-data/gt-imputation/ukb_imp_chr{bgen_contig}_v3.bgen"
    output:
        "prep-data/gt-imputation/ukb_chr{bgen_contig}.zarr"
    params:
        contig_index=lambda wc: bgen_contigs.loc[wc.bgen_contig]['index']
    conda:
        "envs/gwas.yaml"
    log:
        "logs/bgen_to_zarr.{bgen_contig}.txt"
    shell:
        "python scripts/bgen_to_zarr.py {input} {output}"
        
onsuccess:
    print("Workflow finished, no error")

onerror:
    print("An error occurred")