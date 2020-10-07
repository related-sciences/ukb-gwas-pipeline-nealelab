# Rename and subset fields from primary csv as a necessary
# precursor to PHESANT phenotype normalization
rule main_csv_phesant_field_prep:
    input: f"raw-data/main/ukb{ukb_app_id}.csv"
    output: "prep-data/main/ukb_phesant_prep.csv"
    conda: "../envs/phesant.yaml"
    shell:
        "Rscript scripts/external/phesant_phenotype_prep.R {input} {output}"

# Clone hash from PHESANT codebase (there are no documented tags/releases)
rule clone_phesant:
    output:
        directory("rs-ukb/repos/PHESANT")
    shell:
        # Checkout from https://github.com/astheeggeggs/PHESANT/commit/0179587186cbcf53f4536076aa8cb7b1e6435672
        "mkdir repos && cd repos && "
        "git clone https://github.com/astheeggeggs/PHESANT.git && "
        "cd PHESANT && "
        "git checkout 0179587186cbcf53f4536076aa8cb7b1e6435672"
        
# Generate a phenotype subset for experimentation using some common polygenic conditions
rule main_csv_phesant_variable_list:
    input: rules.clone_phesant.output
    # Follow Duncan Palmer's suggestion on the appropriate variable metadata file
    output: "rs-ukb/repos/PHESANT/variable-info/outcome_info_final_pharma_nov2019.tsv-subset01.tsv"
    conda: "../envs/spark.yaml"
    shell:
        "python scripts/create_phesant_variable_list.py run "
        "--input-path={input} "
        "--output-path={output} "

# Run PHESANT phenotype normalization
rule main_csv_phesant_phenotypes:
    input: 
        phesant_repo=rules.clone_phesant.output,
        phenotypes_file=rules.main_csv_phesant_field_prep.output,
        variables_file=rules.main_csv_phesant_variable_list.output,
        coding_file="../variable-info/data-coding-ordinal-info-nov2019-update.txt"
    output: "prep-data/main/ukb_phesant_phenotypes-subset01.csv"
    conda: "../envs/phesant.yaml"
    shell:
        "cd repos/PHESANT/WAS && "
        "rm -rf /tmp/phesant && "
        "mkdir -p /tmp/phesant && "
        "Rscript phenomeScan.r "
        "--phenofile={input.phenotypes_file} "
        "--variablelistfile={input.variables_file} "
        "--datacodingfile=../variable-info/data-coding-ordinal-info-nov2019-update.txt "
        "--userId=userId "
        "--out=phenotypes-subset01 "
        "--resDir=/tmp/phesant "
        "--partIdx=1 "
        "--numParts=1 && "
        "cp /tmp/phesant/phenotypes-subset01.1.tsv {output}"