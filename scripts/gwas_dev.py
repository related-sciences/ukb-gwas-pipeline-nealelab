import logging
import logging.config
import os
from pathlib import Path
from typing import Any, Dict

import dask
import dask.array as da
import fire
import gcsfs
import numpy as np
import pandas as pd
import sgkit
import xarray as xr
from dask.diagnostics import ProgressBar
from dask.distributed import Client
from sgkit.io.bgen.bgen_reader import unpack_variables

# from sgkit.stats import association
from xarray import DataArray, Dataset

fs = gcsfs.GCSFileSystem()

logging.config.fileConfig(Path(__file__).resolve().parents[1] / "log.ini")
logger = logging.getLogger(__name__)


def init():
    # Set this globally to avoid constant warnings like:
    # PerformanceWarning: Slicing is producing a large chunk. To accept the large chunk and silence this warning, set the option
    # >>> with dask.config.set(**{'array.slicing.split_large_chunks': False})
    dask.config.set(**{"array.slicing.split_large_chunks": False})
    ProgressBar().register()
    if "DASK_SCHEDULER_ADDRESS" in os.environ:
        client = Client()
        logger.info(f"Initialized script with dask client:\n{client}")
    else:
        logger.info(
            "Skipping initialization of distributed scheduler "
            "(no `DASK_SCHEDULER_ADDRESS` found in env)"
        )


def get_chunks(ds: Dataset, var: str = "call_genotype_probability") -> Dict[str, int]:
    chunks = dict(zip(ds[var].dims, ds[var].data.chunksize))
    return {d: chunks[d] if d in {"variants", "samples"} else -1 for d in ds.dims}


def load_dataset(
    path: str, unpack: bool = False, consolidated: bool = False
) -> Dataset:
    store = gcsfs.GCSMap(path, gcs=fs, check=False, create=False)
    ds = xr.open_zarr(store, concat_characters=False, consolidated=consolidated)
    if unpack:
        ds = unpack_variables(ds, dtype="float16")
    for v in ds:
        # Workaround for https://github.com/pydata/xarray/issues/4386
        if v.endswith("_mask"):
            ds[v] = ds[v].astype(bool)
    return ds


def save_dataset(ds: Dataset, path: str):
    store = gcsfs.GCSMap(path, gcs=fs, check=False, create=False)
    for v in ds:
        ds[v].encoding.pop("chunks", None)
    ds.to_zarr(store, mode="w", consolidated=True)


def load_sample_qc(sample_qc_path: str) -> Dataset:
    store = gcsfs.mapping.GCSMap(sample_qc_path, gcs=fs, check=True, create=False)
    ds = xr.open_zarr(store, consolidated=True)
    ds = ds.rename_vars(dict(eid="id"))
    ds = ds.rename_vars({v: f"sample_{v}" for v in ds})
    if "sample_sex" in ds:
        # Rename to avoid conflict with bgen field
        ds = ds.rename_vars({"sample_sex": "sample_qc_sex"})
    return ds


def variant_genotype_counts(ds: Dataset) -> DataArray:
    gti = ds["call_genotype_probability"].argmax(dim="genotypes")
    gti = gti.astype("uint8").expand_dims("genotypes", axis=-1)
    gti = gti == da.arange(ds.dims["genotypes"], dtype="uint8")
    return gti.sum(dim="samples", dtype="int32")


def apply_filters(ds: Dataset, filters: Dict[str, Any], dim: str) -> Dataset:
    logger.info("Filter summary (True ==> kept):")
    mask = []
    for k, v in filters.items():
        v = v.compute()
        logger.info(f"\t{k}: {v.to_series().value_counts().to_dict()}")
        mask.append(v.values)
    mask = np.stack(mask, axis=1)
    mask = np.all(mask, axis=1)
    assert len(mask) == ds.dims[dim]
    if len(filters) > 1:
        logger.info(f"\toverall: {pd.Series(mask).value_counts().to_dict()}")
    return ds.isel(**{dim: mask})


TRAIT_ID_COLS = [
    "50",  # Height (https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=50)
    "23098",  # Weight (https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=23098)
]


def load_traits(phenotypes_path: str):
    df = pd.read_csv(phenotypes_path, sep="\t")
    ds = (
        df[["userId"]]
        .rename(columns={"userId": "id"})
        .rename_axis("samples", axis="rows")
        .to_xarray()
        .drop("samples")
    )
    ds["trait"] = xr.DataArray(df[TRAIT_ID_COLS].values, dims=("samples", "traits"))
    # TODO: Decide how to partition phenotypes based on presence, or process them individually
    ds["trait_imputed"] = ds.trait.pipe(
        lambda x: x.where(x.notnull(), x.mean(dim="samples"))
    )
    ds["trait_names"] = xr.DataArray(
        np.array(["height", "weight"], dtype="S"), dims=["traits"]
    )
    ds = ds.rename_vars({v: f"sample_{v}" for v in ds})
    return ds


def add_traits(ds: Dataset, phenotypes_path: str) -> Dataset:
    ds_tr = load_traits(phenotypes_path)
    ds = ds.assign_coords(samples=lambda ds: ds.sample_id).merge(
        ds_tr.assign_coords(samples=lambda ds: ds.sample_id),
        join="left",
        compat="override",
    )
    return ds.reset_index("samples").reset_coords(drop=True)


def add_covariates(ds: Dataset, npc: int = 10) -> Dataset:
    covariates = np.column_stack(
        (
            ds["sample_genetic_sex"],
            ds["sample_age_at_recruitment"],
            ds["sample_principal_component"][:, :npc],
        )
    )
    assert np.all(np.isfinite(covariates))
    ds["sample_covariate"] = xr.DataArray(covariates, dims=("samples", "covariates"))
    ds["sample_covariate"] = ds.sample_covariate.pipe(
        lambda x: (x - x.mean(dim="samples")) / x.std(dim="samples")
    )
    assert np.all(np.isfinite(ds.sample_covariate))
    return ds


SAMPLE_QC_COLS = [
    "sample_id",
    "sample_qc_sex",
    "sample_genetic_sex",
    "sample_age_at_recruitment",
    "sample_principal_component",
    "sample_ethnic_background",
    "sample_genotype_measurement_batch",
    "sample_genotype_measurement_plate",
    "sample_genotype_measurement_well",
]


def apply_sample_qc_1(ds: Dataset, sample_qc_path: str) -> Dataset:
    ds_sqc = load_sample_qc(sample_qc_path)
    ds_sqc = sample_qc_1(ds_sqc)
    ds_sqc = ds_sqc[SAMPLE_QC_COLS]
    ds = ds.assign_coords(samples=lambda ds: ds.sample_id).merge(
        ds_sqc.assign_coords(samples=lambda ds: ds.sample_id).compute(),
        join="inner",
        compat="override",
    )
    return ds.reset_index("samples").reset_coords(drop=True)


def sample_qc_1(ds: Dataset) -> Dataset:
    # See: https://github.com/Nealelab/UK_Biobank_GWAS#imputed-v3-sample-qc
    filters = {
        "no_aneuploidy": ds.sample_sex_chromosome_aneuploidy.isnull(),
        "in_pca": ds.sample_used_in_genetic_principal_components == 1,
        # 1001 = White/British, 1002 = Mixed/Irish
        "in_ethnic_groups": ds.sample_ethnic_background.isin([1001, 1002]),
    }
    return apply_filters(ds, filters, dim="samples")


def variant_qc_1(ds: Dataset) -> Dataset:
    # See: https://github.com/Nealelab/UK_Biobank_GWAS#imputed-v3-variant-qc
    ds["variant_dosage_std"] = ds["call_dosage"].astype("float32").std(dim="samples")
    ds = apply_filters(ds, {"high_info": ds.variant_info > 0.8}, dim="variants")
    #     ds = apply_filters(
    #         ds, {"nonzero_stddev": ds.variant_dosage_std > 0}, dim="variants"
    #     )
    return ds


def variant_qc_2(ds: Dataset) -> Dataset:
    # See: https://github.com/Nealelab/UK_Biobank_GWAS#imputed-v3-variant-qc
    ds["variant_genotype_counts"] = variant_genotype_counts(ds)[
        :, [1, 0, 2]
    ]  # Order: het, hom_ref, hom_alt
    ds = sgkit.hardy_weinberg_test(
        ds, genotype_counts="variant_genotype_counts", ploidy=2
    )
    ds = apply_filters(ds, {"high_maf": ds.variant_maf > 0.001}, dim="variants")
    ds = apply_filters(ds, {"in_hwe": ds.variant_hwe_p_value > 1e-10}, dim="variants")
    return ds


def run_qc_1(input_path: str, output_path: str):
    init()
    logger.info(
        f"Running stage 1 QC (input_path={input_path}, output_path={output_path})"
    )
    ds = load_dataset(input_path, unpack=True)

    logger.info(f"Loaded dataset:\n{ds}")
    chunks = get_chunks(ds)

    logger.info("Applying QC filters")
    ds = variant_qc_1(ds)

    ds = ds.chunk(chunks=chunks)
    logger.info(f"Saving dataset to {output_path}:\n{ds}")
    save_dataset(ds, output_path)
    logger.info("Done")


def run_qc_2(input_path: str, sample_qc_path: str, output_path: str):
    init()
    logger.info(
        f"Running stage 1 QC (input_path={input_path}, output_path={output_path})"
    )
    ds = load_dataset(input_path, consolidated=True)

    logger.info(f"Loaded dataset:\n{ds}")
    chunks = get_chunks(ds)

    logger.info("Applying variant QC filters")
    ds = variant_qc_2(ds)

    logger.info(f"Applying sample QC filters (sample_qc_path={sample_qc_path})")
    ds = apply_sample_qc_1(ds, sample_qc_path=sample_qc_path)

    ds = ds.chunk(chunks=chunks)
    logger.info(f"Saving dataset to {output_path}:\n{ds}")
    save_dataset(ds, output_path)
    logger.info("Done")


# def load_gwas_ds(
#     genotypes_path: str, sample_qc_path: str, phenotypes_path: str
# ) -> Dataset:
#     ds = load_packed_genotype_dataset(genotypes_path)
#     ds = sample_qc(ds, sample_qc_path)
#     ds = variant_qc(ds)
#     ds = add_covariates(ds)
#     ds = add_traits(ds, phenotypes_path)
#     ds = ds[[v for v in sorted(ds)]]
#     return ds


# def run_gwas():
#     # Hard code paths for now
#     genotypes_path = "rs-ukb/prep-data/gt-imputation/ukb_chr21.zarr"
#     sample_qc_path = "rs-ukb/prep-data/main/ukb_sample_qc.zarr"
#     phenotypes_path = "gs://rs-ukb/prep-data/main/phenotypes.v01.subset01.1.tsv"
#     output_path = "/home/eczech/data/rs-ukb-local/gwas/cache/chr21_variant_p_value.n5"

#     logger.info("Running GWAS")
#     client = Client()
#     logger.info(f"Dask client:\n{client}")
#     ds = load_gwas_ds(genotypes_path, sample_qc_path, phenotypes_path)
#     logger.info(f"Loaded dataset:\n{ds}")

#     logger.info("Computing dosage standard deviation")
#     with ProgressBar():
#         ds["variant_dosage_std"] = ds["variant_dosage_std"].compute()

#     logger.info("Computing dosage HWE")
#     with ProgressBar():
#         ds["variant_hwe_p_value"] = ds["variant_hwe_p_value"].compute()

#     ds_qc = apply_variant_qc(ds)

#     logger.info("Running regression")
#     with ProgressBar():
#         ds_gwas = association.gwas_linear_regression(
#             # Promote to f4 to avoid:
#             # TypeError: array type float16 is unsupported in linalg
#             ds_qc.assign(call_dosage=lambda ds: ds_qc.call_dosage.astype("float32")),
#             dosage="call_dosage",
#             covariates="sample_covariate",
#             traits="sample_trait_imputed",
#             add_intercept=True,
#         )

#     logger.info(f"Saving p-values to {output_path}")
#     ds_gwas.variant_p_value.to_netcdf(output_path)
#     logger.info("Done")


if __name__ == "__main__":
    fire.Fire()
