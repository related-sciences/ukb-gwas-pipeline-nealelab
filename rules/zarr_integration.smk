
rule plink_to_zarr:
    input:
        bed_path="raw/gt-calls/ukb_cal_chr{plink_contig}_v2.bed",
        bim_path="raw/gt-calls/ukb_snp_chr{plink_contig}_v2.bim",
        fam_path="raw/gt-calls/ukb59384_cal_chr{plink_contig}_v2_s488264.fam"
    output: 
        "prep/gt-calls/ukb_chr{plink_contig}.ckpt"
    threads: gke_io_ncpu - 1
    resources: mem_mb=gke_io_mem_mb - 5000
    conda: "../envs/io.yaml"
    params:
        zarr_path=lambda wc: bucket_path(f"prep/gt-calls/ukb_chr{wc.plink_contig}.zarr"),
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
    return [f"raw/gt-imputation/ukb59384_imp_chr{wc.bgen_contig}_v3_s{n_samples}.sample"]

rule bgen_to_zarr:
    input:
        bgen_path="raw/gt-imputation/ukb_imp_chr{bgen_contig}_v3.bgen",
        variants_path="raw/gt-imputation/ukb_mfi_chr{bgen_contig}_v3.txt",
        samples_path=bgen_samples_path
    output:
        "prep/gt-imputation/ukb_chr{bgen_contig}.ckpt"
    threads: gke_io_ncpu - 1
    resources: mem_mb=gke_io_mem_mb - 5000
    conda: "../envs/io.yaml"
    params:
        zarr_path=lambda wc: bucket_path(f"prep/gt-imputation/ukb_chr{wc.bgen_contig}.zarr"),
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