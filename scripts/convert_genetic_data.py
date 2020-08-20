"""UKB PLINK/BGEN to Zarr converter"""
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import dask
import fire
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from dask.diagnostics import ProgressBar
from sgkit_bgen import read_bgen
from sgkit_plink import read_plink
from xarray import Dataset

logging.config.fileConfig(Path(__file__).resolve().parents[1] / "log.ini")
logger = logging.getLogger(__name__)


@dataclass
class BGENPaths:
    bgen_path: str
    variants_path: str
    samples_path: str


@dataclass
class PLINKPaths:
    bed_path: str
    bim_path: str
    fam_path: str


@dataclass
class Contig:
    name: str
    index: int


def transform_contig(ds: Dataset, contig: Contig) -> Dataset:
    # Preserve the original contig index/name field
    # in case there are multiple (e.g. PAR1, PAR2 within XY)
    ds["variant_contig_name"] = xr.DataArray(
        np.array(ds.attrs["contigs"])[ds["variant_contig"].values], dims="variants"
    )
    # Overwrite contig index with single value matching index
    # for contig name in file name
    ds["variant_contig"].data = np.full(
        ds["variant_contig"].shape, contig.index, dtype=ds["variant_contig"].dtype
    )
    # Add attributes for convenience
    ds.attrs["contig_name"] = contig.name
    ds.attrs["contig_index"] = contig.index
    return ds


def load_plink(paths: PLINKPaths, contig: Contig) -> Dataset:
    logger.info(f"Loading PLINK dataset for contig {contig} from {paths.bed_path}")
    with dask.config.set(scheduler="threads"):
        ds = read_plink(
            bed_path=paths.bed_path,
            bim_path=paths.bim_path,
            fam_path=paths.fam_path,
            bim_int_contig=False,
            count_a1=False,
        )
    ds["sample_id"] = ds["sample_id"].astype("int32")
    # All useful sample metadata will come from the
    # main UKB dataset instead
    ds = ds.drop_vars(
        [
            "sample_family_id",
            "sample_paternal_id",
            "sample_maternal_id",
            "sample_phenotype",
        ]
    )
    # Update contig index/names
    ds = transform_contig(ds, contig)
    return ds


def load_bgen_variants(path: str) -> Dataset:
    # See: https://github.com/Nealelab/UK_Biobank_GWAS/blob/8f8ee456fdd044ce6809bb7e7492dc98fd2df42f/0.1/09.load_mfi_vds.py
    cols = [
        ("id", str),
        ("rsid", str),
        ("position", "int32"),
        ("allele1_ref", str),
        ("allele2_alt", str),
        ("maf", float),
        ("minor_allele", str),
        ("info", float),
    ]
    df = pd.read_csv(path, sep="\t", names=[c[0] for c in cols], dtype=dict(cols))
    ds = df.rename_axis("variants", axis="rows").to_xarray().drop("variants")
    ds = ds.rename({v: "variant_" + v for v in ds})
    return ds


def load_bgen_samples(path: str) -> Dataset:
    cols = [("id1", "int32"), ("id2", "int32"), ("missing", str), ("sex", str)]
    # Example .sample file:
    # head ~/data/rs-ukb/raw-data/gt-imputation/ukb59384_imp_chr4_v3_s487296.sample
    # ID_1 ID_2 missing sex
    # 0 0 0 D
    # 123123 123123 0 1  # Actual ids replaced with fake numbers
    df = pd.read_csv(
        path,
        sep=" ",
        dtype=dict(cols),
        names=[c[0] for c in cols],
        header=0,
        skiprows=1,  # Skip the first non-header row
    )
    ds = df.rename_axis("samples", axis="rows").to_xarray().drop("samples")
    ds = ds.rename({v: "sample_" + v for v in ds})
    return ds


def load_bgen_dosage(
    path: str, contig: Contig, chunks: Optional[Union[str, int, tuple]] = None
) -> Dataset:

    if chunks is None:
        n_bytes = 536870912  # 512MiB
        n_variants = 1024
        n_samples = (n_bytes // 4) // n_variants
        chunks = (n_variants, n_samples)

    ds = read_bgen(path, chunks=chunks)

    # Update contig index/names
    ds = transform_contig(ds, contig)

    # Drop most variables since the external tables are more useful
    ds = ds[
        ["variant_contig", "variant_contig_name", "call_dosage", "call_dosage_mask"]
    ]
    return ds


def load_bgen(
    paths: BGENPaths, contig: Contig, chunks: Optional[Union[str, int, tuple]] = None
) -> Dataset:
    dsd = load_bgen_dosage(paths.bgen_path, contig, chunks=chunks)
    dsv = load_bgen_variants(paths.variants_path)
    dss = load_bgen_samples(paths.samples_path)
    # Note that attrs are dropped by default on merge; no_conflicts
    # will merge any with unlike names or same names with equal values
    return xr.merge([dsv, dss, dsd], combine_attrs="no_conflicts")


def save_dataset(
    output_path: str,
    ds: Dataset,
    contig: Contig,
    scheduler: str = "threads",
    remote: bool = True,
):
    store = output_path
    if remote:
        import gcsfs

        gcs = gcsfs.GCSFileSystem()
        store = gcsfs.GCSMap(output_path, gcs=gcs, check=False, create=True)
    compressor = zarr.Blosc(cname="zstd", clevel=3, shuffle=2)
    encoding = {v: {"compressor": compressor} for v in ds}
    logger.info(f"Dataset for contig {contig}:\n{ds}")
    logger.info(
        f"Writing dataset for contig {contig} to {output_path} (scheduler={scheduler}, remote={remote})"
    )
    with dask.config.set(scheduler=scheduler), ProgressBar():
        ds.to_zarr(store=store, mode="w", consolidated=True, encoding=encoding)


def plink_to_zarr(
    input_path_bed: str,
    input_path_bim: str,
    input_path_fam: str,
    output_path: str,
    contig_name: str,
    contig_index: int,
    scheduler: str = "processes",
    remote: bool = True,
):
    """Convert UKB PLINK to Zarr"""
    paths = PLINKPaths(
        bed_path=input_path_bed, bim_path=input_path_bim, fam_path=input_path_fam
    )
    contig = Contig(name=contig_name, index=contig_index)
    ds = load_plink(paths, contig)
    save_dataset(output_path, ds, contig, scheduler=scheduler, remote=remote)


def bgen_to_zarr(
    input_path_bgen: str,
    input_path_variants: str,
    input_path_samples: str,
    output_path: str,
    contig_name: str,
    contig_index: int,
    scheduler: str = "processes",
    remote: bool = True,
):
    """Convert UKB BGEN to Zarr"""
    paths = BGENPaths(
        bgen_path=input_path_bgen,
        variants_path=input_path_variants,
        samples_path=input_path_samples,
    )
    contig = Contig(name=contig_name, index=contig_index)
    ds = load_bgen(paths, contig)
    save_dataset(output_path, ds, contig, scheduler=scheduler, remote=remote)


if __name__ == "__main__":
    fire.Fire()
