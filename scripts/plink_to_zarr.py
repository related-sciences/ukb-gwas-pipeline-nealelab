"""PLINK to Zarr converter"""
import logging
import os.path as osp

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

def load_contig(paths, contig):
    contig_name, contig_index = contig['name'], contig['index']
    logger.info(f"Loading dataset for contig {contig} from {paths['bed_path']}")
    with dask.config.set(scheduler="threads"):
        ds = read_plink(**paths, bim_int_contig=False, count_a1=False)
    ds["sample_id"] = ds["sample_id"].astype("int32")
    ds = ds.rename_vars({"variant_alleles": "variant_allele"})
    ds = ds.drop_vars(
        [
            "sample_family_id",
            "sample_paternal_id",
            "sample_maternal_id",
            "sample_phenotype",
        ]
    )
    ds["variant_contig"].data = np.full(
        ds["variant_contig"].shape, contig_index, dtype=ds["variant_contig"].dtype
    )
    for k in contig:
        ds.attrs[f"contig_{k}"] = contig[k]
    return ds


def write_contig(output_path, gcs, ds, contig):
    store = gcsfs.GCSMap(output_path, gcs=gcs, check=False, create=True)
    compressor = zarr.Blosc(cname="zstd", clevel=3, shuffle=2)
    logger.info(f"Dataset for contig {contig}:\n{ds}")
    encoding = {v: {"compressor": compressor} for v in ds}
    logger.info(f"Writing dataset for contig {contig} to {output_path}")
    with dask.config.set(scheduler="processes"), ProgressBar():
        ds.to_zarr(store=store, mode="w", consolidated=True, encoding=encoding)


def run(input_path_bed: str, input_path_bim: str, input_path_fam: str, output_path: str, contig_name: str, contig_index: int):
    """Convert UKB PLINK to Zarr"""
    paths = dict(bed_path=input_path_bed, bim_path=input_path_bim, fam_path=input_path_fam)
    contig = dict(name=contig_name, index=contig_index)
    gcs = gcsfs.GCSFileSystem()
    ds = load_contig(paths, contig)
    write_contig(output_path, gcs, ds, contig)


if __name__ == "__main__":
    fire.Fire()
