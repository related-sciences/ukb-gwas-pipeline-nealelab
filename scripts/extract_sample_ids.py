import logging
import logging.config
from pathlib import Path

import fire
import fsspec
import pandas as pd
import xarray as xr

logging.config.fileConfig(Path(__file__).resolve().parents[1] / "log.ini")
logger = logging.getLogger(__name__)


def get_sample_ids(path):
    ds = xr.open_zarr(fsspec.get_mapper(path))
    return ds.sample_id.to_series().to_list()


def run(input_path, output_path):
    logger.info(f"Extracting sample ids from {input_path} into {output_path}")
    ids = get_sample_ids(input_path)
    ids = pd.DataFrame(dict(sample_id=list(set(ids))))
    ids.to_csv(output_path, sep="\t", index=False)
    logger.info(f"Number of sample ids saved: {len(ids)}")
    logger.info("Done")


if __name__ == "__main__":
    fire.Fire()
