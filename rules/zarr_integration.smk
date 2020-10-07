import pandas as pd

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
        "../envs/io.yaml"
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

def get_thread_ct(key):
    return config['env'][key] - 1
    
def get_mem_mb(n_cpu, frac=1., mem_per_cpu_mb=4000):
    return int(frac * n_cpu * mem_per_cpu_mb)

rule bgen_to_zarr:
    input:
        bgen_path="raw-data/gt-imputation/ukb_imp_chr{bgen_contig}_v3.bgen",
        variants_path="raw-data/gt-imputation/ukb_mfi_chr{bgen_contig}_v3.txt",
        samples_path=bgen_samples_path
    output:
        "prep-data/gt-imputation/ukb_chr{bgen_contig}.ckpt"
    threads: get_thread_ct('gke_io_ncpu')
    conda: "../envs/io.yaml"
    resources:
        mem_mb=get_mem_mb(get_thread_ct('gke_io_ncpu'))
    params:
        zarr_path=lambda wc: bucket_path(f"prep-data/gt-imputation/ukb_chr{wc.bgen_contig}.zarr"),
        contig_index=lambda wc: bgen_contigs.loc[str(wc.bgen_contig)]['index']
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