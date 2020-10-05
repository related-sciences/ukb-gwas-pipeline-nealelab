rule import_ot_nealelab_sumstats:
    input:
        GS.remote('genetics-portal-raw/uk_biobank_sumstats/neale_v2/output/neale_v2_sumstats/{ukb_field_id}_raw.neale2.gwas.imputed_v3.both_sexes.tsv.gz', user_project=gcp_project)
     output:
        "pipe-data/external/ot_sumstats/{ukb_field_id}_raw.neale2.gwas.imputed_v3.both_sexes.tsv.gz"
     shell:
        "gsutil -u {gcp_project} -m rsync -r gs://{params.input_path} gs://{params.output_path} && touch {output}"
