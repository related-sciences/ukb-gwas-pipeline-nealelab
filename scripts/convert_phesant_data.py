"""Script to convert PHESANT output to parquet/zarr"""
import logging
import logging.config
from pathlib import Path

import fire
import numpy as np
import pandas as pd

logging.config.fileConfig(Path(__file__).resolve().parents[1] / "log.ini")
logger = logging.getLogger(__name__)


def to_parquet(input_path: str, output_path: str):
    from pyspark.sql import SparkSession

    logger.info(f"Converting csv at {input_path} to {output_path}")
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv(input_path, sep="\t", header=True, inferSchema=True)
    df.write.mode("overwrite").parquet(output_path, compression="snappy")
    logger.info("Done")


def to_zarr(input_path: str, output_path: str, dictionary_path: str):
    import dask.dataframe as dd
    import fsspec
    import xarray as xr

    logger.info(f"Converting parquet at {input_path} to {output_path}")
    df = dd.read_parquet(input_path)

    trait_columns = df.columns[df.columns.to_series().str.match(r"^\d+")]
    # 41210_Z942 -> 41210 (UKB field id)
    trait_group_ids = [c.split("_")[0] for c in trait_columns]
    # 41210_Z942 -> Z942 (Data coding value as one-hot encoding in phenotype, e.g.)
    trait_code_ids = ["_".join(c.split("_")[1:]) for c in trait_columns]
    trait_values = df[trait_columns].astype("float").to_dask_array()
    trait_values.compute_chunk_sizes()

    trait_id_to_name = (
        pd.read_csv(
            dictionary_path,
            sep=",",
            usecols=["FieldID", "Field"],
            dtype={"FieldID": str, "Field": str},
        )
        .set_index("FieldID")["Field"]
        .to_dict()
    )
    trait_name = [trait_id_to_name.get(v) for v in trait_group_ids]

    ds = xr.Dataset(
        dict(
            id=("samples", np.asarray(df["userId"], dtype=int)),
            trait=(("samples", "traits"), trait_values),
            trait_id=("traits", np.asarray(trait_columns.values, dtype=str)),
            trait_group_id=("traits", np.array(trait_group_ids, dtype=int)),
            trait_code_id=("traits", np.array(trait_code_ids, dtype=str)),
            trait_name=("traits", np.array(trait_name, dtype=str)),
        )
    )
    # Keep chunks small in trait dimension for faster per-trait processing
    ds["trait"] = ds["trait"].chunk(dict(samples="auto", traits=100))
    ds = ds.rename_vars({v: f"sample_{v}" for v in ds})

    logger.info(f"Saving dataset to {output_path}:\n{ds}")
    ds.to_zarr(fsspec.get_mapper(output_path), consolidated=True, mode="w")
    logger.info("Done")


def sort_zarr(input_path: str, genotypes_path: str, output_path: str):
    import fsspec
    import xarray as xr

    ds_tr = xr.open_zarr(fsspec.get_mapper(input_path), consolidated=True)
    ds_gt = xr.open_zarr(fsspec.get_mapper(genotypes_path), consolidated=True)

    # Sort trait data using genomic data sample ids;
    # Note that this will typically produce a warning like:
    # "PerformanceWarning: Slicing with an out-of-order index is generating 69909 times more chunks"
    # which is OK since the purpose of this step is to incur this cost once
    # instead of many times in a repetitive GWAS workflow
    ds = ds_tr.set_index(samples="sample_id").sel(samples=ds_gt.sample_id)
    ds = ds.rename_vars({"samples": "sample_id"}).reset_coords("sample_id")

    # Restore chunkings; reordered traits array will have many chunks
    # of size 1 in samples dim without this
    for v in ds_tr:
        ds[v] = ds[v].chunk(ds_tr[v].data.chunksize)

    ds.to_zarr(fsspec.get_mapper(output_path), consolidated=True, mode="w")


if __name__ == "__main__":
    fire.Fire()
