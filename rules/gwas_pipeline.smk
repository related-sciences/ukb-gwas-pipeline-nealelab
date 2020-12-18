# Apply simple variant QC filters
rule qc_filter_stage_1:
    input: "prep/gt-imputation/ukb_chr{bgen_contig}.ckpt"
    output: "prep/gt-imputation-qc/ukb_chr{bgen_contig}.ckpt"
    params: 
        input_path=lambda wc: bucket_path(f"prep/gt-imputation/ukb_chr{wc.bgen_contig}.zarr", True),
        output_path=lambda wc: bucket_path(f"prep/gt-imputation-qc/ukb_chr{wc.bgen_contig}.zarr", True)
    conda: "../envs/gwas.yaml"
    shell:
        "python scripts/gwas.py run_qc_1 "
        "--input-path={params.input_path} "
        "--output-path={params.output_path} "
        "&& touch {output}"
        
# Apply more complex QC filters
rule qc_filter_stage_2:
    input: 
        "prep/gt-imputation-qc/ukb_chr{bgen_contig}.ckpt",
        "prep/main/ukb_sample_qc.ckpt"
    output: "pipe/nealelab-gwas-uni-ancestry-v3/input/gt-imputation/ukb_chr{bgen_contig}.ckpt"
    params: 
        input_path=lambda wc: bucket_path(f"prep/gt-imputation-qc/ukb_chr{wc.bgen_contig}.zarr", True),
        sample_qc_path=bucket_path("prep/main/ukb_sample_qc.zarr", True),
        output_path=lambda wc: bucket_path(f"pipe/nealelab-gwas-uni-ancestry-v3/input/gt-imputation/ukb_chr{wc.bgen_contig}.zarr", True)
    conda: "../envs/gwas.yaml"
    shell:
        "python scripts/gwas.py run_qc_2 "
        "--sample-qc-path={params.sample_qc_path} "
        "--input-path={params.input_path} "
        "--output-path={params.output_path} "
        "&& touch {output}"

# Run regressions
rule gwas:
    input:
        genotypes_ckpt="pipe/nealelab-gwas-uni-ancestry-v3/input/gt-imputation/ukb_chr{bgen_contig}.ckpt",
        phenotypes_ckpt="pipe/nealelab-gwas-uni-ancestry-v3/input/main/ukb_phesant_phenotypes.ckpt"
    output: 
        "pipe/nealelab-gwas-uni-ancestry-v3/output/gt-imputation/ukb_chr{bgen_contig}.ckpt"
    params:
        genotypes_path=lambda wc: bucket_path(f"pipe/nealelab-gwas-uni-ancestry-v3/input/gt-imputation/ukb_chr{wc.bgen_contig}.zarr", True),
        phenotypes_path=lambda wc: bucket_path(f"pipe/nealelab-gwas-uni-ancestry-v3/input/main/ukb_phesant_phenotypes.zarr", True),
        sumstats_path=lambda wc: bucket_path(f"pipe/nealelab-gwas-uni-ancestry-v3/output/gt-imputation/sumstats/ukb_chr{wc.bgen_contig}", True),
        variables_path=lambda wc: bucket_path(f"pipe/nealelab-gwas-uni-ancestry-v3/output/gt-imputation/variables/ukb_chr{wc.bgen_contig}", True)
    conda: "../envs/gwas.yaml"
    shell:
        "python scripts/gwas.py run_gwas "
        "--genotypes-path={params.genotypes_path} "
        "--phenotypes-path={params.phenotypes_path} "
        "--sumstats-path={params.sumstats_path} "
        "--variables-path={params.variables_path} "
        "&& touch {output}"

# "--trait-group-ids=[1990,20095] "
# "--trait-group-ids=[50,23098] "
    
# Combine sumstats produced here with those from external source (Open Targets)
rule sumstats:
    output: "pipe/nealelab-gwas-uni-ancestry-v3/output/sumstats-1990-20095.parquet"
    #output: "pipe/nealelab-gwas-uni-ancestry-v3/output/sumstats-50-23098.parquet"
    #output: "pipe/nealelab-gwas-uni-ancestry-v3/output/sumstats.parquet"
    params:
        # Note: lambda is important for braces in format to not be interpreted as snakemake wildcards
        gwas_sumstats_path=bucket_path("pipe/nealelab-gwas-uni-ancestry-v3/output/gt-imputation/sumstats", True),
        ot_sumstats_path=bucket_path("external/ot_nealelab_sumstats", True),
        contigs="[21]",
        trait_group_ids="[1990,20095]"
    conda: "../envs/gwas.yaml"
    shell:
        "python scripts/merge_sumstats.py run "
        "--gwas-sumstats-path={params.gwas_sumstats_path} "
        "--ot-sumstats-path={params.ot_sumstats_path} "
        "--output-path={output} "
        "--contigs={params.contigs} "
        "--trait_group_ids={params.trait_group_ids} "