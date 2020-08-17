import pandas as pd
# import pipeline as pl
# zarr output: path = osp.join(data_dir, f"ukb_chr{contig_name}.zarr")

configfile: "config-test.yaml"

plink_contigs = pd.DataFrame(config['plink']['contigs']).set_index('name', drop=False)

rule all:
    input:
        expand(
            "data/prep-data/gt-calls/ukb_chr{plink_contig}.zarr", 
            plink_contig=plink_contigs['name']
        )

rule step1:
    input:
        bed_path="data/raw-data/gt-calls/ukb_cal_chr{plink_contig}_v2.bed",
        bim_path="data/raw-data/gt-calls/ukb_snp_chr{plink_contig}_v2.bim",
        fam_path="data/raw-data/gt-calls/ukb59384_cal_chr{plink_contig}_v2_s488264.fam"
    output:
        path="data/prep-data/gt-calls/ukb_chr{plink_contig}.zarr"
    params:
        contig_index=lambda wc: plink_contigs.loc[wc.plink_contig]['index']
    shell:
       "python scripts/plink_to_zarr.py run "
       "--bed-path={input.bed_path} "
       "--bim-path={input.bim_path} "
       "--fam-path={input.fam_path} "
       "--output-path={output.path} "
       "--contig-name={wildcards.plink_contig} "
       "--contig-index={params.contig_index}"