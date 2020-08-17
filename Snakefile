import pipeline as pl


# zarr output: path = osp.join(data_dir, f"ukb_chr{contig_name}.zarr")

rule step1:
    input:
       "data/" + pl.BED_FMT,
    output:
       "data/cli1.txt"
    conda:
       "envs/env1.yaml"
    shell:
       "python cli1.py run --x=3 --y=10"