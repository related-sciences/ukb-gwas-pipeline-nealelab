"""UKB PLINK/BGEN to Zarr conversion functions"""
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Tuple, Union

import dask
import fire
import gcsfs
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from dask.diagnostics import ProgressBar
from sgkit.io.bgen import read_bgen, rechunk_bgen
from sgkit.io.plink import read_plink
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
        np.array(ds.attrs["contigs"])[ds["variant_contig"].values].astype("S"),
        dims="variants",
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


def load_bgen_variants(path: str):
    # See: https://github.com/Nealelab/UK_Biobank_GWAS/blob/8f8ee456fdd044ce6809bb7e7492dc98fd2df42f/0.1/09.load_mfi_vds.py
    cols = [
        ("id", str),
        ("rsid", str),
        ("position", "int32"),
        ("allele1_ref", str),
        ("allele2_alt", str),
        ("maf", "float32"),
        ("minor_allele", str),
        ("info", "float32"),
    ]
    df = pd.read_csv(path, sep="\t", names=[c[0] for c in cols], dtype=dict(cols))
    ds = df.rename_axis("variants", axis="rows").to_xarray().drop("variants")
    ds["allele"] = xr.concat([ds["allele1_ref"], ds["allele2_alt"]], dim="alleles").T
    ds = ds.drop_vars(["allele1_ref", "allele2_alt"])
    for c in cols + [("allele", str)]:
        if c[0] in ds and c[1] == str:
            ds[c[0]] = ds[c[0]].compute().astype("S")
    ds = ds.rename({v: "variant_" + v for v in ds})
    ds = ds.chunk(chunks="auto")
    return ds


def load_bgen_samples(path: str) -> Dataset:
    cols = [("id1", "int32"), ("id2", "int32"), ("missing", str), ("sex", "uint8")]
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
    # id1 always equals id2 and missing is always 0
    df = df[["id1", "sex"]].rename(columns={"id1": "id"})
    ds = df.rename_axis("samples", axis="rows").to_xarray().drop("samples")
    ds = ds.rename({v: "sample_" + v for v in ds})
    return ds


def load_bgen_probabilities(
    path: str, contig: Contig, chunks: Optional[Union[str, int, tuple]] = None
) -> Dataset:
    ds = read_bgen(path, chunks=chunks, gp_dtype="float16")

    # Update contig index/names
    ds = transform_contig(ds, contig)

    # Drop most variables since the external tables are more useful
    ds = ds[
        [
            "variant_contig",
            "variant_contig_name",
            "call_genotype_probability",
            "call_genotype_probability_mask",
        ]
    ]
    return ds


def load_bgen(
    paths: BGENPaths,
    contig: Contig,
    region: Optional[Tuple[int, int]] = None,
    variant_info_threshold: Optional[float] = None,
    chunks: Tuple[int, int] = (250, -1),
):
    logger.info(
        f"Loading BGEN dataset for contig {contig} from "
        f"{paths.bgen_path} (chunks = {chunks})"
    )
    # Load and merge primary + axis datasets
    dsp = load_bgen_probabilities(paths.bgen_path, contig, chunks=chunks + (-1,))
    dsv = load_bgen_variants(paths.variants_path)
    dss = load_bgen_samples(paths.samples_path)
    ds = xr.merge([dsv, dss, dsp], combine_attrs="no_conflicts")

    # Apply variant slice if provided
    if region is not None:
        logger.info(f"Applying filter to region {region}")
        n_variant = ds.dims["variants"]
        ds = ds.isel(variants=slice(region[0], region[1]))
        logger.info(f"Filtered to {ds.dims['variants']} variants of {n_variant}")

    # Apply variant info threshold if provided (this is applied
    # early because it is not particularly controversial and
    # eliminates ~80% of variants when near .8)
    if variant_info_threshold is not None:
        logger.info(f"Applying filter to variant info > {variant_info_threshold}")
        n_variant = ds.dims["variants"]
        ds = ds.isel(variants=ds.variant_info > variant_info_threshold)
        logger.info(f"Filtered to {ds.dims['variants']} variants of {n_variant}")
        # Make sure to rechunk after non-uniform filter
        for v in ds:
            if "variants" in ds[v].dims and "samples" in ds[v].dims:
                ds[v] = ds[v].chunk(chunks=dict(variants=chunks[0]))
            elif "variants" in ds[v].dims:
                ds[v] = ds[v].chunk(chunks=dict(variants="auto"))

    return ds


def rechunk_dataset(
    ds: Dataset,
    output: str,
    contig: Contig,
    fn: Callable,
    chunks: Tuple[int, int],
    max_mem: str,
    progress_update_seconds: int = 60,
    remote: bool = True,
    **kwargs,
) -> Dataset:
    logger.info(
        f"Rechunking dataset for contig {contig} "
        f"to {output} (chunks = {chunks}):\n{ds}"
    )

    if remote:
        gcs = gcsfs.GCSFileSystem()
        output = gcsfs.GCSMap(output, gcs=gcs, check=False, create=False)

    # Save to local zarr store with desired sample chunking
    with ProgressBar(dt=progress_update_seconds):
        res = fn(
            ds,
            output=output,
            chunk_length=chunks[0],
            chunk_width=chunks[1],
            max_mem=max_mem,
            **kwargs,
        )

    logger.info(f"Rechunked dataset:\n{res}")
    return res


def save_dataset(
    output_path: str,
    ds: Dataset,
    contig: Contig,
    scheduler: str = "threads",
    remote: bool = True,
    progress_update_seconds: int = 60,
):
    store = output_path
    if remote:
        gcs = gcsfs.GCSFileSystem()
        store = gcsfs.GCSMap(output_path, gcs=gcs, check=False, create=False)
    logger.info(
        f"Dataset to save for contig {contig}:\n{ds}\n"
        f"Writing dataset for contig {contig} to {output_path} "
        f"(scheduler={scheduler}, remote={remote})"
    )
    with dask.config.set(scheduler=scheduler), dask.config.set(
        {"optimization.fuse.ave-width": 50}
    ), ProgressBar(dt=progress_update_seconds):
        ds.to_zarr(store=store, mode="w", consolidated=True)


def plink_to_zarr(
    input_path_bed: str,
    input_path_bim: str,
    input_path_fam: str,
    output_path: str,
    contig_name: str,
    contig_index: int,
    remote: bool = True,
):
    """Convert UKB PLINK to Zarr"""
    paths = PLINKPaths(
        bed_path=input_path_bed, bim_path=input_path_bim, fam_path=input_path_fam
    )
    contig = Contig(name=contig_name, index=contig_index)
    ds = load_plink(paths, contig)
    # TODO: Switch to rechunk method
    save_dataset(output_path, ds, contig, scheduler="processes", remote=remote)
    logger.info("Done")


def bgen_to_zarr(
    input_path_bgen: str,
    input_path_variants: str,
    input_path_samples: str,
    output_path: str,
    contig_name: str,
    contig_index: int,
    max_mem: str = "1GB",  # per-worker
    remote: bool = True,
    region: Optional[Tuple[int, int]] = None,
):
    """Convert UKB BGEN to Zarr"""
    paths = BGENPaths(
        bgen_path=input_path_bgen,
        variants_path=input_path_variants,
        samples_path=input_path_samples,
    )
    contig = Contig(name=contig_name, index=contig_index)
    ds = load_bgen(paths, contig, region=region)

    # Chosen with expected shape across all chroms (~128MB chunks):
    # normalize_chunks('auto', shape=(97059328, 487409), dtype='float32')
    chunks = (5216, 5792)
    ds = rechunk_dataset(
        ds,
        output=output_path,
        contig=contig,
        fn=rechunk_bgen,
        chunks=chunks,
        max_mem=max_mem,
        remote=remote,
        compressor=zarr.Blosc(cname="zstd", clevel=7, shuffle=2, blocksize=0),
        probability_dtype="uint8",
        pack=True,
    )
    logger.info("Done")


if __name__ == "__main__":
    fire.Fire()
