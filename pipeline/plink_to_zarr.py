"""PLINK to Zarr converter"""
import logging
import os.path as osp
from typing import List, Optional

import dask
import fire
import gcsfs
import numpy as np
import ukb_analysis as ukb
import zarr
from dask.diagnostics import ProgressBar
from sgkit_plink import read_plink

logging.config.fileConfig("log.ini")
logger = logging.getLogger(__name__)

BED_FMT = "ukb_cal_chr{contig}_v2.bed"
BIM_FMT = "ukb_snp_chr{contig}_v2.bim"
FAM_FMT = "ukb59384_cal_chr{contig}_v2_s488264.fam"


def load_contig(data_dir, contig):
    contig_name = contig["name"]
    contig_index = contig["index"]
    paths = (
        osp.join(data_dir, BED_FMT.format(contig=contig_name)),
        osp.join(data_dir, BIM_FMT.format(contig=contig_name)),
        osp.join(data_dir, FAM_FMT.format(contig=contig_name)),
    )
    logger.info(f"Loading dataset for contig {contig} from {paths[0]}")
    with dask.config.set(scheduler="threads"):
        ds = read_plink(path=paths, bim_int_contig=False, count_a1=False)
    ds["sample/id"] = ds["sample/id"].astype("int32")
    ds = ds.rename_vars({"variant/alleles": "variant/allele"})
    ds = ds.drop_vars(
        [
            "sample/family_id",
            "sample/paternal_id",
            "sample/maternal_id",
            "sample/phenotype",
        ]
    )
    ds["variant/contig"].data = np.full(
        ds["variant/contig"].shape, contig_index, dtype=ds["variant/contig"].dtype
    )
    for k in contig:
        ds.attrs[f"contig_{k}"] = contig[k]
    return ds


def write_contig(data_dir, gcs, ds, contig):
    contig_name = contig["name"]
    path = osp.join(data_dir, f"ukb_chr{contig_name}.zarr")
    store = gcsfs.GCSMap(path, gcs=gcs, check=False, create=True)
    compressor = zarr.Blosc(cname="zstd", clevel=3, shuffle=2)
    ds = ds.rename_vars({v: v.replace("/", "_") for v in ds})
    logger.info(f"Dataset for contig {contig}:\n{ds}")
    encoding = {v: {"compressor": compressor} for v in ds}
    logger.info(f"Writing dataset for contig {contig} to {path}")
    with dask.config.set(scheduler="processes"), ProgressBar():
        ds.to_zarr(store=store, mode="w", consolidated=True, encoding=encoding)


def run(input_path: str, output_path: str, contigs: Optional[List[int]] = None):
    """Convert UKB PLINK to Zarr

    Parameters
    ----------
    input_path : str
        Local path containing PLINK datasets, e.g.
        `$HOME/data/rs-ukb/raw-data/gt-calls`
    output_path : str
        Path to zarr results, e.g.
        `gs://rs-ukb/prep-data/gt-calls`
    contigs : Optional[List[int]]
        List of contig codes (1-based) to convert,
        by default all will be converted
    """
    config = ukb.get_config()
    gcs = gcsfs.GCSFileSystem()
    for contig in config["contigs"]:
        if contigs and contig["code"] not in contigs:
            continue
        ds = load_contig(input_path, contig)
        write_contig(output_path, gcs, ds, contig)
    logger.info("Done")


if __name__ == "__main__":
    fire.Fire()
