import logging
import logging.config
import re
from pathlib import Path
from typing import Optional, Sequence, Union

import fire
import fsspec
import numpy as np
import pandas as pd

logging.config.fileConfig(Path(__file__).resolve().parents[1] / "log.ini")
logger = logging.getLogger(__name__)

###########################
# Sgkit sumstat functions #


def get_gwas_sumstat_manifest(path: str) -> pd.DataFrame:
    store = fsspec.get_mapper(path)
    df = []
    for f in list(store):
        fn = f.split("/")[-1]
        parts = re.findall(r"ukb_chr(\d+)_(\d+)_(.*).parquet", fn)
        if not parts:
            continue
        parts = parts[0]
        df.append(
            dict(
                contig=parts[0],
                batch=int(parts[1]),
                trait_id=parts[2],
                trait_group_id=parts[2].split("_")[0],
                trait_code_id="_".join(parts[2].split("_")[1:]),
                file=f,
            )
        )
    return pd.DataFrame(df)


def load_gwas_sumstats(path: str) -> pd.DataFrame:
    logger.info(f"Loading GWAS sumstats from {path}")
    return (
        pd.read_parquet(path)
        .rename(columns=lambda c: c.replace("sample_", ""))
        .rename(columns={"variant_contig_name": "contig"})
        # b'21' -> '21'
        .assign(contig=lambda df: df["contig"].str.decode("utf-8"))
        # b'21:9660864_G_A' -> '21:9660864:G:A'
        .assign(
            variant_id=lambda df: df["variant_id"]
            .str.decode("utf-8")
            .str.replace("_", ":")
        )
        .drop(["variant_index", "trait_index", "variant_contig"], axis=1)
        .rename(columns={"variant_p_value": "p_value"})
        .set_index(["trait_id", "contig", "variant_id"])
        .add_prefix("gwas_")
    )


########################
# OT sumstat functions #


def get_ot_sumstat_manifest(path: str) -> pd.DataFrame:
    # See https://github.com/related-sciences/ukb-gwas-pipeline-nealelab/issues/31 for example paths
    store = fsspec.get_mapper(path)
    files = list(store)
    df = []
    for f in files:
        if not f.endswith(".tsv.gz"):
            continue
        trait_id = f.split("/")[-1].split(".")[0]
        if trait_id.endswith("_irnt"):
            # Ignore rank normalized continuous outcomes in favor of "raw" outcomes
            continue
        if trait_id.endswith("_raw"):
            trait_id = trait_id.replace("_raw", "")
        df.append(
            dict(
                trait_id=trait_id,
                trait_group_id=trait_id.split("_")[0],
                trait_code_id="_".join(trait_id.split("_")[1:]),
                file=f,
            )
        )
    return pd.DataFrame(df)


OT_COLS = [
    "chromosome",
    "variant",
    "variant_id",
    "minor_allele",
    "n_complete_samples",
    "beta",
    "p-value",
    "tstat",
]


def load_ot_trait_sumstats(path: str, row: pd.Series) -> pd.DataFrame:
    path = path + "/" + row["file"]
    logger.info(f"Loading OT sumstats from {path}")
    return (
        pd.read_csv(path, sep="\t", usecols=OT_COLS, dtype={"chromosome": str},)
        .rename(columns={"variant_id": "variant_rsid"})
        .rename(
            columns={
                "chromosome": "contig",
                "p-value": "p_value",
                "variant": "variant_id",
            }
        )
        .assign(
            trait_id=row["trait_id"],
            trait_group_id=row["trait_group_id"],
            trait_code_id=row["trait_code_id"],
        )
        .set_index(["trait_id", "contig", "variant_id"])
        .add_prefix("ot_")
    )


def load_ot_sumstats(
    path: str, df: pd.DataFrame, contigs: Sequence[str]
) -> pd.DataFrame:
    df = pd.concat([load_ot_trait_sumstats(path, row) for _, row in df.iterrows()])
    df = df.loc[df.index.get_level_values("contig").isin(contigs)]
    return df


#########
# Merge #


def run(
    gwas_sumstats_path: str,
    ot_sumstats_path: str,
    output_path: str,
    contigs: Optional[Union[str, Sequence[str]]] = None,
    trait_group_ids: Optional[Union[str, Sequence[str]]] = None,
):
    df_sg = get_gwas_sumstat_manifest(
        "gs://rs-ukb/pipe/nealelab-gwas-uni-ancestry-v3/output/gt-imputation/sumstats"
    )
    df_ot = get_ot_sumstat_manifest("gs://rs-ukb/external/ot_nealelab_sumstats")

    def prep_filter(v, default_values):
        if isinstance(v, str):
            v = v.split(",")
        if v is None:
            v = default_values
        return [str(e) for e in v]

    contigs = prep_filter(contigs, df_sg["contig"].unique())
    trait_group_ids = prep_filter(trait_group_ids, df_sg["trait_group_id"].unique())
    logger.info(f"Using {len(contigs)} contigs (first 10: {contigs[:10]})")
    logger.info(
        f"Using {len(trait_group_ids)} trait_group_ids (first 10: {trait_group_ids[:10]})"
    )

    def apply_trait_filter(df):
        return df[df["trait_group_id"].isin(trait_group_ids)]

    def apply_contig_filter(df):
        return df[df["contig"].isin(contigs)]

    df_sg, df_ot = df_sg.pipe(apply_trait_filter), df_ot.pipe(apply_trait_filter)
    df_sg = df_sg.pipe(apply_contig_filter)

    # Only load OT sumstats for traits present in GWAS comparison data
    df_ot = df_ot[df_ot["trait_group_id"].isin(df_sg["trait_group_id"].unique())]

    logger.info(f"Loading GWAS sumstats ({len(df_sg)} partitions)")
    df_sg = pd.concat(
        [load_gwas_sumstats(gwas_sumstats_path + "/" + f) for f in df_sg["file"]]
    )
    df_sg.info()

    logger.info(f"Loading OT sumstats ({len(df_ot)} partitions)")
    df_ot = load_ot_sumstats(ot_sumstats_path, df_ot, contigs)
    df_ot.info()

    logger.info("Merging")
    assert df_sg.index.unique
    assert df_ot.index.unique
    df = pd.concat([df_sg, df_ot], axis=1, join="outer")
    df["gwas_log_p_value"] = -np.log10(df["gwas_p_value"])
    df["ot_log_p_value"] = -np.log10(df["ot_p_value"])
    df = df[sorted(df)]

    logger.info(f"Saving result to {output_path}:\n")
    df.info()
    df.to_parquet(output_path)
    logger.info("Done")


if __name__ == "__main__":
    fire.Fire()
