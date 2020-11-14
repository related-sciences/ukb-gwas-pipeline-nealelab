# Apply simple variant QC filters
rule qc_filter_stage_1:
    input: "prep/gt-imputation/ukb_chr{bgen_contig}.ckpt"
    output: "prep/gt-imputation-qc/ukb_chr{bgen_contig}.ckpt"
    params: 
        input_path=lambda wc: bucket_path(f"prep/gt-imputation/ukb_chr{wc.bgen_contig}.zarr"),
        output_path=lambda wc: bucket_path(f"prep/gt-imputation-qc/ukb_chr{wc.bgen_contig}.zarr")
    conda: "../envs/gwas.yaml"
    shell:
        "python scripts/gwas.py run_qc_1 "
        "--input-path={params.input_path} "
        "--output-path={params.output_path} "
        "&& touch {output}"
        
# Apply more complex QC filters
rule qc_filter_stage_2:
    input: "prep/gt-imputation-qc/ukb_chr{bgen_contig}.ckpt"
    output: "pipe/nealelab-gwas-uni-ancestry-v3/input/gt-imputation/ukb_chr{bgen_contig}.ckpt"
    params: 
        input_path=lambda wc: bucket_path(f"prep/gt-imputation-qc/ukb_chr{wc.bgen_contig}.zarr"),
        output_path=lambda wc: bucket_path(f"pipe/nealelab-gwas-uni-ancestry-v3/input/gt-imputation/ukb_chr{wc.bgen_contig}.zarr"),
        sample_qc_path=bucket_path("prep/main/ukb_sample_qc.zarr")
    conda: "../envs/gwas.yaml"
    shell:
        "python scripts/gwas.py run_qc_2 "
        "--sample-qc-path={params.sample_qc_path} "
        "--input-path={params.input_path} "
        "--output-path={params.output_path} "
        "&& touch {output}"

rule gwas:
    input:
        genotypes_ckpt="pipe/nealelab-gwas-uni-ancestry-v3/input/gt-imputation/ukb_chr{bgen_contig}.ckpt",
        phenotypes_path="prep/main/ukb_phesant_phenotypes-subset01.csv"
    output: 
        "pipe/nealelab-gwas-uni-ancestry-v3/output/gt-imputation/ukb_chr{bgen_contig}.ckpt"
    params:
        genotypes_path=lambda wc: bucket_path(f"pipe/nealelab-gwas-uni-ancestry-v3/input/gt-imputation/ukb_chr{wc.bgen_contig}.zarr"),
        output_path=lambda wc: bucket_path(f"pipe/nealelab-gwas-uni-ancestry-v3/output/gt-imputation/ukb_chr{wc.bgen_contig}")
    conda: "../envs/gwas.yaml"
    shell:
        "python scripts/gwas.py run_gwas "
        "--genotypes-path={params.genotypes_path} "
        "--phenotypes-path={input.phenotypes_path} "
        "--output-path={params.output_path} "
        "&& touch {output}"
    