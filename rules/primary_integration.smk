# Convert primary UKB csv to parquet
rule main_csv_to_parquet:
    # Takes ~45 mins on 4 cores, 12g heap
    input: f"raw-data/main/ukb{ukb_app_id}.csv"
    output: "prep-data/main/ukb.ckpt"
    conda: "../envs/spark.yaml"
    params:
        parquet_path=bucket_path("prep-data/main/ukb.parquet")
    shell:
        "export SPARK_DRIVER_MEMORY=12g && "
        "python ../scripts/convert_main_data.py csv_to_parquet "
        "--input-path={input} "
        "--output-path={params.parquet_path} && "
        "gsutil -m -q rsync -d -r {params.parquet_path} gs://{params.parquet_path} && "
        "touch {output}"
        
rule download_data_dictionary:
    input: HTTP.remote("biobank.ctsu.ox.ac.uk/~bbdatan/Data_Dictionary_Showcase.csv")
    output: "pipe-data/external/ukb_meta/data_dictionary_showcase.csv"
    shell: "mv {input} {output}"
    
rule extract_sample_qc_csv:
    input: rules.main_csv_to_parquet.output
    output: "prep-data/main/ukb_sample_qc.csv"
    conda: "../envs/spark.yaml"
    params: 
        input_path=bucket_path("prep-data/main/ukb.parquet")
    shell:
        "export SPARK_DRIVER_MEMORY=10g && "
        "python ../scripts/extract_main_data.py sample_qc_csv "
        "--input-path={params.input_path} "
        "--output-path={output} "
        
rule extract_sample_qc_zarr:
    input: rules.extract_sample_qc_csv.output
    output: "prep-data/main/ukb_sample_qc.ckpt"
    conda: "../envs/gwas.yaml"
    params:
        output_path=bucket_path("prep-data/main/ukb_sample_qc.zarr")
    shell:
        "python ../scripts/extract_main_data.py sample_qc_zarr "
        "--input-path={input} "
        "--output-path={params.output_path} "
        "--remote=True && "
        "touch {output}"

# Direct sample matches may not be necessary for reproduction. Leave this
# here though until we can be sure that the sample ids are recoverable through
# custom QC steps:
# rule extract_sample_sets:
#     input:
#         # This was downloaded manually from the NealeLab results spreadsheet
#         # before all the Dropbox links broke
#         # See: https://docs.google.com/spreadsheets/d/1kvPoupSzsSFBNSztMzl04xMoSC3Kcx3CrjVf4yBmESU/edit?ts=5b5f17db#gid=178908679
#         # TODO: Update to download as separate step when it's possible to get these files
#         european_samples="pipe-data/external/nealelab_v3_20180731/european_samples.tsv"
#     output:
#         # pipe-data/nealelab_rapid_gwas/import
#         # pipe-data/nealelab_rapid_gwas/run/202008
#         "pipe-data/external/nealelab_v3_20180731/extract/sample_sets.csv"
#     conda:
#         "envs/spark.yaml"
#     shell:
#         "export SPARK_DRIVER_MEMORY=10g && "
#         "python scripts/extract_external_data.py nlv3_sample_sets "
#         "--input-path-european-samples={input.european_samples} "
#         "--output-path={output}"