"""BGEN to Zarr converter"""
import logging

import dask
import fire
import numpy as np
import zarr
from dask.diagnostics import ProgressBar
from sgkit_bgen import read_bgen

logging.config.fileConfig("log.ini")
logger = logging.getLogger(__name__)


def run(input_path, output_path):
    print("In bgen_to_zarr:", input_path, output_path)
    from pathlib import Path

    Path(output_path).touch()


if __name__ == "__main__":
    fire.Fire()
