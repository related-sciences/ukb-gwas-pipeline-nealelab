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
    input: "prep/gt-imputation-qc/ukb_chr{bgen_contig}.ckpt"
    output: "pipe/nealelab-gwas-uni-ancestry-v3/input/gt-imputation/ukb_chr{bgen_contig}.ckpt"
    params: 
        input_path=lambda wc: bucket_path(f"prep/gt-imputation-qc/ukb_chr{wc.bgen_contig}.zarr", True),
        output_path=lambda wc: bucket_path(f"pipe/nealelab-gwas-uni-ancestry-v3/input/gt-imputation/ukb_chr{wc.bgen_contig}.zarr", True),
        sample_qc_path=bucket_path("prep/main/ukb_sample_qc.zarr", True)
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
        phenotypes_ckpt="prep/main/ukb_phesant_phenotypes-subset01.csv"
    output: 
        "pipe/nealelab-gwas-uni-ancestry-v3/output/gt-imputation/ukb_chr{bgen_contig}.ckpt"
    params:
        genotypes_path=lambda wc: bucket_path(f"pipe/nealelab-gwas-uni-ancestry-v3/input/gt-imputation/ukb_chr{wc.bgen_contig}.zarr", True),
        phenotypes_path=lambda wc: bucket_path("prep/main/ukb_phesant_phenotypes-subset01.csv", True),
        dictionary_path=lambda wc: bucket_path("prep/main/meta/data_dictionary_showcase.csv", True),
        output_path=lambda wc: bucket_path(f"pipe/nealelab-gwas-uni-ancestry-v3/output/gt-imputation/ukb_chr{wc.bgen_contig}", True)
    conda: "../envs/gwas.yaml"
    shell:
        "python scripts/gwas.py run_gwas "
        "--genotypes-path={params.genotypes_path} "
        "--phenotypes-path={params.phenotypes_path} "
        "--dictionary-path={params.dictionary_path} "
        "--output-path={params.output_path} "
        "&& touch {output}"
    
    
rule sumstat_merge:
    output: "pipe/nealelab-gwas-uni-ancestry-v3/output/sumstats.parquet"
    params:
        # Note: lambda is important for braces in format to not be interpreted as snakemake wildcards
        gwas_sumstats_path_fmt=lambda wc: bucket_path("pipe/nealelab-gwas-uni-ancestry-v3/output/gt-imputation/ukb_chr{contig}/sumstats.parquet", True),
        ot_sumstats_path_fmt=lambda wc: bucket_path("external/ot_nealelab_sumstats/{trait_id}_raw.neale2.gwas.imputed_v3.both_sexes.tsv.gz", True),
        # TODO: decide how this should be parameterized -- it should be all contigs at some point 
        contigs="[21]"
    conda: "../envs/gwas.yaml"
    shell:
        "python scripts/validation.py merge_sumstats "
        "--gwas-sumstats-path-fmt='{params.gwas_sumstats_path_fmt}' "
        "--ot-sumstats-path-fmt='{params.ot_sumstats_path_fmt}' "
        "--output-path={output} "
        "--contigs={params.contigs} "