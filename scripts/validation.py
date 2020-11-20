import logging
import logging.config
from pathlib import Path
from typing import Sequence, Tuple, Union

import fire
import numpy as np
import pandas as pd

logging.config.fileConfig(Path(__file__).resolve().parents[1] / "log.ini")
logger = logging.getLogger(__name__)


def load_gwas_sumstats(path_fmt: str, contig: str) -> pd.DataFrame:
    return (
        pd.read_parquet(path_fmt.format(contig=contig))
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


def load_ot_trait_sumstats(path_fmt: str, trait_id: str) -> pd.DataFrame:
    return (
        pd.read_csv(
            path_fmt.format(trait_id=trait_id),
            sep="\t",
            usecols=OT_COLS,
            dtype={"chromosome": str},
        )
        .rename(columns={"variant_id": "variant_rsid"})
        .rename(
            columns={
                "chromosome": "contig",
                "p-value": "p_value",
                "variant": "variant_id",
            }
        )
        .assign(trait_id=trait_id)
        .set_index(["trait_id", "contig", "variant_id"])
        .add_prefix("ot_")
    )


def load_ot_sumstats(
    path_fmt: str, trait_ids: Sequence[str], contigs: Sequence[str]
) -> pd.DataFrame:
    df = pd.concat(
        [load_ot_trait_sumstats(path_fmt, trait_id) for trait_id in trait_ids]
    )
    df = df.loc[df.index.get_level_values("contig").isin(contigs)]
    return df


def merge_sumstats(
    gwas_sumstats_path_fmt: str,
    ot_sumstats_path_fmt: str,
    output_path: str,
    contigs: Union[str, Tuple[str, ...]],
):
    if isinstance(contigs, str):
        contigs = contigs.split(",")
    contigs = [str(c) for c in contigs]

    logger.info(
        f"Merging sumstats (gwas_sumstats_path_fmt={gwas_sumstats_path_fmt}, "
        f"ot_sumstats_path_fmt={ot_sumstats_path_fmt}, contigs={contigs})"
    )

    logger.info("Loading GWAS sumstats")
    df1 = pd.concat(
        [load_gwas_sumstats(gwas_sumstats_path_fmt, contig) for contig in contigs]
    )
    df1.info()

    trait_ids = df1.index.get_level_values("trait_id").unique()

    logger.info("Loading OT sumstats")
    df2 = load_ot_sumstats(ot_sumstats_path_fmt, trait_ids, contigs)
    df2.info()

    logger.info("Merging")
    assert df1.index.unique
    assert df2.index.unique
    df = pd.concat([df1, df2], axis=1, join="outer")
    df["gwas_log_p_value"] = -np.log10(df["gwas_p_value"])
    df["ot_log_p_value"] = -np.log10(df["ot_p_value"])
    df = df[sorted(df)]

    logger.info(f"Saving result to {output_path}:\n")
    df.info()
    df.to_parquet(output_path)
    logger.info("Done")


if __name__ == "__main__":
    fire.Fire()
