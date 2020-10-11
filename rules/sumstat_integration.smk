# Copy sumstats to avoid repeated access on request pays bucket
rule import_ot_nealelab_sumstats:
    output: 'external/ot_nealelab_sumstats/copy.ckpt'
    params: output_dir = bucket_path('external/ot_nealelab_sumstats')
    shell:
        "gsutil -u {gcp_project} -m cp "
        "gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/output/neale_v2_sumstats/*_raw.neale2.gwas.imputed_v3.both_sexes.tsv.gz "
        "gs://{params.output_dir}/ && "
        "touch {output}"