# Rename and subset fields from primary csv as a necessary
# precursor to PHESANT phenotype normalization
rule main_csv_phesant_field_prep:
    input: f"raw/main/ukb.csv"
    output: "prep/main/ukb_phesant.csv"
    conda: "../envs/phesant.yaml"
    shell:
        "Rscript scripts/external/phesant_phenotype_prep.R {input} {output}"
        

# Dump genetic data QC samples
rule extract_gwas_qc_sample_ids:
    # Note: autosome sample ids are superset of XY contig sample ids and all autosomes have same samples
    input: "pipe/nealelab-gwas-uni-ancestry-v3/input/gt-imputation/ukb_chr22.ckpt"
    output: "pipe/nealelab-gwas-uni-ancestry-v3/input/sample_ids.csv"
    conda: "../envs/gwas.yaml"
    params:
        input_path=bucket_path("pipe/nealelab-gwas-uni-ancestry-v3/input/gt-imputation/ukb_chr22.zarr", True)
    shell:
        "python scripts/extract_sample_ids.py run {params.input_path} {output}"

# Subset phenotype data to genetic data QC samples
rule filter_phesant_csv:
    input: 
        sample_id_path=rules.extract_gwas_qc_sample_ids.output,
        input_path=rules.main_csv_phesant_field_prep.output
    output: "prep/main/ukb_phesant_filtered.csv"
    conda: "../envs/gwas.yaml"
    shell:
        "python scripts/extract_main_data.py phesant_qc_csv "
        "--input-path={input.input_path} "
        "--sample-id-path={input.sample_id_path} "
        "--output-path={output} "

# Clone hash from PHESANT codebase (there are no documented tags/releases)
rule phesant_clone:
    output:
        directory("temp/repos/PHESANT")
    shell:
        # Fork with updates from https://github.com/astheeggeggs/PHESANT/commit/0179587186cbcf53f4536076aa8cb7b1e6435672
        "mkdir -p {output} && cd {output}/.. && "
        "git clone https://github.com/eric-czech/PHESANT.git && "
        "cd PHESANT && "
        "git checkout 05997a79c734a0706f7622e8c9c734984f1da130"

# Generate a phenotype subset for experimentation using some common polygenic conditions
# rule main_csv_phesant_variable_list:
#     input: rules.phesant_clone.output
#     # Follow Duncan Palmer's suggestion on the appropriate variable metadata file
#     output:"temp/repos/PHESANT/variable-info/outcome_info_final_pharma_nov2019.tsv-subset01.tsv"
#     conda: "../envs/spark.yaml"
#     shell:
#         "python scripts/create_phesant_variable_list.py run "
#         "--input-path={input} "
#         "--output-path={output} "

# Run PHESANT phenotype normalization
rule main_csv_phesant_phenotypes:
    input: 
        # Note: You cannot link this to output of rule that created it as a directory,
        # it must be some file or you will get a "MissingInputException" error.  You 
        # can specify that this is a directory, but then a warning is thrown like
        # "cannot use directory as input".
        phesant_repo="temp/repos/PHESANT/README.md",
        phenotypes_file=rules.filter_phesant_csv.output
    output: "prep/main/ukb_phesant_phenotypes.csv"
    params:
        variables_file="../variable-info/outcome_info_final_pharma_nov2019.tsv",
        coding_file="../variable-info/data-coding-ordinal-info-nov2019-update.txt"
    conda: "../envs/phesant.yaml"
    shell:
        # These hacks handle package installations that were not valid according to conda 
        # dependency rules (i.e. optparse + r + data.table of any version) and symlink to tsv in 
        # order to alter delimiter expected in R script
        "R -e \"install.packages(c('optparse', 'bit64'))\" && "
        "OUTPUT_FILE=`readlink -f {output}` && "
        "PHENOTYPE_FILE_CSV=`readlink -f {input.phenotypes_file}` && "
        "PHENOTYPE_FILE_TSV=`echo ${{PHENOTYPE_FILE_CSV%.csv}}.tsv` && "
        "ln -sf $PHENOTYPE_FILE_CSV $PHENOTYPE_FILE_TSV && "
        "cd `dirname {input.phesant_repo}`/WAS && "
        "rm -rf /tmp/phesant && "
        "mkdir -p /tmp/phesant && "
        "Rscript phenomeScan.r "
        "--phenofile=$PHENOTYPE_FILE_TSV "
        "--variablelistfile={params.variables_file} "
        "--datacodingfile={params.coding_file} "
        "--userId=userId "
        "--out=phenotypes "
        "--resDir=/tmp/phesant "
        "--partIdx=1 "
        "--numParts=1 && "
        "cp /tmp/phesant/phenotypes.1.tsv $OUTPUT_FILE"


# Extract the numeric UKB field ids in columns as a separate csv
rule main_csv_phesant_phenotypes_field_id_export:
    input: rules.main_csv_phesant_phenotypes.output
    output: rules.main_csv_phesant_phenotypes.output[0].replace('.csv', '.field_ids.csv')
    run:
        field_ids = []
        with open(input[0], "r") as f:
            headers = f.readline().split("\t")
            headers = sorted(set(map(lambda v: v.split('_')[0], headers)))
            for field_id in headers:
                field_id = field_id.replace('"', '').strip()
                try:
                    field_id = int(field_id)
                except ValueError:
                    continue
                field_ids.append(field_id)
        with open(output[0], "w") as f:
            f.write("ukb_field_id\n")
            for field_id in field_ids:
                f.write(str(field_id) + "\n")
