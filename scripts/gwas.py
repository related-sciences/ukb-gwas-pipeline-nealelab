import logging
import logging.config
import os
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse

import dask
import dask.array as da
import fire
import fsspec
import numpy as np
import pandas as pd
import sgkit as sg
import xarray as xr
from dask.diagnostics import ProgressBar
from dask.distributed import Client, get_task_stream, performance_report
from sgkit.io.bgen.bgen_reader import unpack_variables
from xarray import DataArray, Dataset

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


def add_protocol(url, protocol="gs"):
    if not urlparse(str(url)).scheme:
        return protocol + "://" + url
    return url


def get_chunks(ds: Dataset, var: str = "call_genotype_probability") -> Dict[str, int]:
    chunks = dict(zip(ds[var].dims, ds[var].data.chunksize))
    return {d: chunks[d] if d in {"variants", "samples"} else -1 for d in ds.dims}


def load_dataset(
    path: str, unpack: bool = False, consolidated: bool = False
) -> Dataset:
    store = fsspec.get_mapper(path, check=False, create=False)
    ds = xr.open_zarr(store, concat_characters=False, consolidated=consolidated)
    if unpack:
        ds = unpack_variables(ds, dtype="float16")
    for v in ds:
        # Workaround for https://github.com/pydata/xarray/issues/4386
        if v.endswith("_mask"):
            ds[v] = ds[v].astype(bool)
    return ds


def save_dataset(ds: Dataset, path: str):
    store = fsspec.get_mapper(path, check=False, create=False)
    for v in ds:
        ds[v].encoding.pop("chunks", None)
    ds.to_zarr(store, mode="w", consolidated=True)


def load_sample_qc(sample_qc_path: str) -> Dataset:
    store = fsspec.get_mapper(sample_qc_path, check=False, create=False)
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
    logger.info("Filter summary (True = kept, False = removed):")
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


TRAIT_IDS = [
    "50",  # Height (https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=50)
    "23098",  # Weight (https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=23098)
]


def load_traits(phenotypes_path: str, dictionary_path: str):
    df = pd.read_csv(phenotypes_path, sep="\t")
    ds = (
        df[["userId"]]
        .rename(columns={"userId": "id"})
        .rename_axis("samples", axis="rows")
        .to_xarray()
        .drop("samples")
    )
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
    ds["trait"] = xr.DataArray(df[TRAIT_IDS].values, dims=("samples", "traits"))
    ds["trait_id"] = xr.DataArray(np.array(TRAIT_IDS, dtype=str), dims="traits")
    ds["trait_imputed"] = ds.trait.pipe(
        lambda x: x.where(x.notnull(), x.mean(dim="samples"))
    )
    ds["trait_name"] = xr.DataArray(
        np.array(
            [trait_id_to_name[trait_id] for trait_id in ds["trait_id"].values],
            dtype=str,
        ),
        dims="traits",
    )
    ds = ds.rename_vars({v: f"sample_{v}" for v in ds})
    return ds


def add_traits(ds: Dataset, phenotypes_path: str, dictionary_path: str) -> Dataset:
    ds_tr = load_traits(phenotypes_path, dictionary_path)
    ds = ds.assign_coords(samples=lambda ds: ds.sample_id).merge(
        ds_tr.assign_coords(samples=lambda ds: ds.sample_id),
        join="left",
        compat="override",
    )
    return ds.reset_index("samples").reset_coords(drop=True)


def add_covariates(ds: Dataset, npc: int = 10) -> Dataset:
    covariates = np.concatenate(
        [
            ds["sample_genetic_sex"].values[:, np.newaxis],
            ds["sample_principal_component"].values[:, :npc],
        ],
        axis=1,
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
    ds = apply_filters(ds, {"high_info": ds.variant_info > 0.8}, dim="variants")
    return ds


def variant_qc_2(ds: Dataset) -> Dataset:
    # See: https://github.com/Nealelab/UK_Biobank_GWAS#imputed-v3-variant-qc
    ds["variant_genotype_counts"] = variant_genotype_counts(ds)[
        :, [1, 0, 2]
    ]  # Order: het, hom_ref, hom_alt
    ds = sg.hardy_weinberg_test(ds, genotype_counts="variant_genotype_counts", ploidy=2)
    ds = apply_filters(ds, {"high_maf": ds.variant_maf > 0.001}, dim="variants")
    ds = apply_filters(ds, {"in_hwe": ds.variant_hwe_p_value > 1e-10}, dim="variants")

    return ds


def run_qc_1(input_path: str, output_path: str):
    init()
    logger.info(
        f"Running stage 1 QC (input_path={input_path}, output_path={output_path})"
    )
    ds = load_dataset(input_path, unpack=False, consolidated=False)

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
    ds = load_dataset(input_path, unpack=True, consolidated=True)

    logger.info(f"Loaded dataset:\n{ds}")
    chunks = get_chunks(ds)

    logger.info("Applying variant QC filters")
    ds = variant_qc_2(ds)

    # Drop probability since it is very large and was only necessary
    # for computing QC-specific quantities
    ds = ds.drop_vars(["call_genotype_probability", "call_genotype_probability_mask"])

    logger.info(f"Applying sample QC filters (sample_qc_path={sample_qc_path})")
    ds = apply_sample_qc_1(ds, sample_qc_path=sample_qc_path)

    ds = ds.chunk(chunks=chunks)
    logger.info(f"Saving dataset to {output_path}:\n{ds}")
    save_dataset(ds, output_path)
    logger.info("Done")


def load_gwas_ds(
    genotypes_path: str, phenotypes_path: str, dictionary_path: str
) -> Dataset:
    ds = load_dataset(genotypes_path, consolidated=True)
    ds = add_covariates(ds)
    ds = add_traits(ds, phenotypes_path, dictionary_path)
    ds = ds[[v for v in sorted(ds)]]
    return ds


def run_trait_gwas(
    ds: Dataset, trait_index: int, trait_id: str, trait_name: str
) -> pd.DataFrame:
    ds = (
        ds
        # Promote to f4 to avoid:
        # TypeError: array type float16 is unsupported in linalg
        .assign(call_dosage=lambda ds: ds.call_dosage.astype("float32"))
        # Subset to specific trait for regression
        .assign(sample_trait_target=lambda ds: ds["sample_trait"][:, trait_index])
        # Drop any other variables with the traits dimensions since they will
        # not merge with a result having only one element in that dimension
        .drop_dims("traits")
        # Filter to complete cases
        .pipe(lambda ds: ds.isel(samples=ds["sample_trait_target"].notnull().values))
    )
    sample_size = ds.dims["samples"]

    logger.info(
        f"Running GWAS for trait '{trait_name}' (index={trait_index}, id={trait_id}) with {sample_size} samples"
    )

    ds = sg.gwas_linear_regression(
        ds,
        dosage="call_dosage",
        covariates="sample_covariate",
        traits="sample_trait_target",
        add_intercept=True,
        merge=True,
    )

    # Project and convert to data frame for convenience
    # in downstream analysis/comparisons
    df = (
        ds[["variant_id", "variant_contig", "variant_contig_name", "variant_p_value"]]
        # This is necessary prior to `to_dask_dataframe`
        # .unify_chunks().to_dask_dataframe()
        .to_dataframe()
        .reset_index()
        .assign(trait_name=trait_name, trait_id=trait_id, sample_size=sample_size)
        .rename(columns={"traits": "trait_index", "variants": "variant_index"})
    )
    return df


def run_gwas(
    genotypes_path: str, phenotypes_path: str, dictionary_path: str, output_path: str
):
    init()

    logger.info(
        f"Running GWAS (genotypes_path={genotypes_path}, phenotypes_path={phenotypes_path}, dictionary_path={dictionary_path}, output_path={output_path})"
    )

    ds = load_gwas_ds(genotypes_path, phenotypes_path, dictionary_path)

    # Rechunk dosage (from 5216 x 5792 @ TOW) down to something smaller in the
    # variants dimension since variant_chunk x n_sample arrays need to
    # fit in memory for linear regression (652 * 365941 * 4 = 954MB)
    ds["call_dosage"] = ds["call_dosage"].chunk(chunks=(652, 5792))

    logger.info(f"Loaded dataset:\n{ds}")

    results = []
    trait_names = ds["sample_trait_name"].values
    trait_ids = ds["sample_trait_id"].values
    for trait_index, trait_id in enumerate(trait_ids):
        trait_name = trait_names[trait_index]
        with performance_report(
            filename=f"/tmp/gwas-{trait_name}-performance-report.html"
        ), get_task_stream(filename=f"/tmp/gwas-{trait_name}-task-stream.html"):
            df = run_trait_gwas(ds, trait_index, trait_id, trait_name)
            results.append(df)
    df = pd.concat(results)

    sumstats_path = output_path + "/sumstats.parquet"
    logger.info(f"Saving GWAS results to {sumstats_path}:\n")
    df.info()
    df.to_parquet(sumstats_path)

    ds = ds[
        [
            "variant_contig",
            "variant_contig_name",
            "variant_id",
            "variant_rsid",
            "variant_position",
            "variant_allele",
            "variant_minor_allele",
            "variant_hwe_p_value",
            "variant_maf",
            "variant_info",
            "sample_id",
            "sample_principal_component",
            "sample_covariate",
            "sample_genetic_sex",
            "sample_age_at_recruitment",
            "sample_ethnic_background",
            "sample_trait",
            "sample_trait_id",
            "sample_trait_imputed",
            "sample_trait_name",
        ]
    ]
    variables_path = output_path + "/variables.zarr"
    logger.info(f"Saving GWAS variables to {variables_path}:\n{ds}")
    save_dataset(ds, variables_path)

    logger.info("Done")


if __name__ == "__main__":
    fire.Fire()
