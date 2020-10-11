# Rename and subset fields from primary csv as a necessary
# precursor to PHESANT phenotype normalization
rule main_csv_phesant_field_prep:
    input: f"raw/main/ukb{ukb_app_id}.csv"
    output: "prep/main/ukb_phesant_prep.csv"
    conda: "../envs/phesant.yaml"
    shell:
        "Rscript scripts/external/phesant_phenotype_prep.R {input} {output}"

# Clone hash from PHESANT codebase (there are no documented tags/releases)
rule phesant_clone:
    output:
        "temp/repos/PHESANT"
    shell:
        # Checkout from https://github.com/astheeggeggs/PHESANT/commit/0179587186cbcf53f4536076aa8cb7b1e6435672
        "mkdir -p {output} && cd {output}/.. && "
        "git clone https://github.com/astheeggeggs/PHESANT.git && "
        "cd PHESANT && "
        "git checkout 0179587186cbcf53f4536076aa8cb7b1e6435672"

# Generate a phenotype subset for experimentation using some common polygenic conditions
rule main_csv_phesant_variable_list:
    input: rules.phesant_clone.output
    # Follow Duncan Palmer's suggestion on the appropriate variable metadata file
    output:"temp/repos/PHESANT/variable-info/outcome_info_final_pharma_nov2019.tsv-subset01.tsv"
    conda: "../envs/spark.yaml"
    shell:
        "python scripts/create_phesant_variable_list.py run "
        "--input-path={input} "
        "--output-path={output} "

# Run PHESANT phenotype normalization
rule main_csv_phesant_phenotypes:
    input: 
        phesant_repo=rules.phesant_clone.output,
        phenotypes_file=rules.main_csv_phesant_field_prep.output,
        variables_file=rules.main_csv_phesant_variable_list.output,
        coding_file="temp/repos/PHESANT/variable-info/data-coding-ordinal-info-nov2019-update.txt"
    output: "prep/main/ukb_phesant_phenotypes-subset01.csv"
    conda: "../envs/phesant.yaml"
    shell:
        "cd rs-ukb/temp/repos/PHESANT/WAS && "
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
