import logging
import logging.config
from pathlib import Path

import fire
import fsspec
import numpy as np
import pandas as pd
import xarray as xr

logging.config.fileConfig(Path(__file__).resolve().parents[1] / "log.ini")
logger = logging.getLogger(__name__)


def get_ot_trait_groups(path):
    store = fsspec.get_mapper(path)
    ids = []
    for file in list(store):
        if not file.endswith(".tsv.gz"):
            continue
        ids.append(int(file.split(".")[0].split("_")[0]))
    return np.unique(ids)


def get_gwas_trait_groups(path):
    ds = xr.open_zarr(fsspec.get_mapper(path))
    return ds["sample_trait_group_id"].to_series().astype(int).unique()


def run(phenotypes_path: str, sumstats_path: str, output_path: str):
    logger.info(
        f"Extracting intersecting traits from {phenotypes_path} and {sumstats_path} to {output_path}"
    )
    ids2 = get_ot_trait_groups(sumstats_path)
    ids1 = get_gwas_trait_groups(phenotypes_path)
    ids = pd.DataFrame(dict(trait_group_id=np.intersect1d(ids1, ids2)))
    ids.to_csv(output_path, sep="\t", index=False)
    logger.info(f"Number of trait group ids saved: {len(ids)}")
    logger.info("Done")


if __name__ == "__main__":
    fire.Fire()
