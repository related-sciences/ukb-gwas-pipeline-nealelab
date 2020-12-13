# Convert primary UKB csv to parquet
rule main_csv_to_parquet:
    # Takes ~45 mins on 4 cores, 12g heap
    input: f"raw-data/main/ukb{ukb_app_id}.csv"
    output: "prep/main/ukb.ckpt"
    conda: "../envs/spark.yaml"
    params:
        parquet_path=bucket_path("prep/main/ukb.parquet")
    shell:
        "export SPARK_DRIVER_MEMORY=12g && "
        "python scripts/convert_main_data.py csv_to_parquet "
        "--input-path={input} "
        "--output-path={params.parquet_path} && "
        "gsutil -m -q rsync -d -r {params.parquet_path} gs://{params.parquet_path} && "
        "touch {output}"
        
rule download_data_dictionary:
    input: HTTP.remote("biobank.ctsu.ox.ac.uk/~bbdatan/Data_Dictionary_Showcase.csv")
    output: "prep/main/meta/data_dictionary_showcase.csv"
    shell: "mv {input} {output}"
    
rule extract_sample_qc_csv:
    input: rules.main_csv_to_parquet.output
    output: "prep/main/ukb_sample_qc.csv"
    conda: "../envs/spark.yaml"
    params: 
        input_path=bucket_path("prep/main/ukb.parquet")
    shell:
        "export SPARK_DRIVER_MEMORY=10g && "
        "python scripts/extract_main_data.py sample_qc_csv "
        "--input-path={params.input_path} "
        "--output-path={output} "
        
rule extract_sample_qc_zarr:
    input: rules.extract_sample_qc_csv.output
    output: "prep/main/ukb_sample_qc.ckpt"
    conda: "../envs/gwas.yaml"
    params:
        output_path=bucket_path("prep/main/ukb_sample_qc.zarr")
    shell:
        "python scripts/extract_main_data.py sample_qc_zarr "
        "--input-path={input} "
        "--output-path={params.output_path} "
        "--remote=True && "
        "touch {output}"