import logging
import logging.config
import os
import time
import traceback
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Union
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
from dask.distributed import Client
from retrying import retry
from sgkit.io.bgen.bgen_reader import unpack_variables
from xarray import DataArray, Dataset

logging.config.fileConfig(Path(__file__).resolve().parents[1] / "log.ini")
logger = logging.getLogger(__name__)

fs = fsspec.filesystem("gs")


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


def save_dataset(ds: Dataset, path: str, retries: int = 3):
    store = fsspec.get_mapper(path, check=False, create=False)
    for v in ds:
        ds[v].encoding.pop("chunks", None)
    task = ds.to_zarr(store, mode="w", consolidated=True, compute=False)
    task.compute(retries=retries)


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


def add_traits(ds: Dataset, phenotypes_path: str) -> Dataset:
    ds_tr = load_dataset(phenotypes_path, consolidated=True)
    ds = ds.assign_coords(samples=lambda ds: ds.sample_id).merge(
        ds_tr.assign_coords(samples=lambda ds: ds.sample_id),
        join="left",
        compat="override",
    )
    return ds.reset_index("samples").reset_coords(drop=True)


def add_covariates(ds: Dataset, npc: int = 20) -> Dataset:
    # See https://github.com/Nealelab/UK_Biobank_GWAS/blob/67289386a851a213f7bb470a3f0f6af95933b041/0.1/22.run_regressions.py#L71
    ds = (
        ds.assign(
            sample_age_at_recruitment_2=lambda ds: ds["sample_age_at_recruitment"] ** 2
        )
        .assign(
            sample_sex_x_age=lambda ds: ds["sample_genetic_sex"]
            * ds["sample_age_at_recruitment"]
        )
        .assign(
            sample_sex_x_age_2=lambda ds: ds["sample_genetic_sex"]
            * ds["sample_age_at_recruitment_2"]
        )
    )
    covariates = np.column_stack(
        [
            ds["sample_age_at_recruitment"].values,
            ds["sample_age_at_recruitment_2"].values,
            ds["sample_genetic_sex"].values,
            ds["sample_sex_x_age"].values,
            ds["sample_sex_x_age_2"].values,
            ds["sample_principal_component"].values[:, :npc],
        ]
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
    # See:
    # - https://github.com/Nealelab/UK_Biobank_GWAS#imputed-v3-sample-qc
    # - https://github.com/Nealelab/UK_Biobank_GWAS/blob/master/0.1/04.subset_samples.py
    filters = {
        "no_aneuploidy": ds.sample_sex_chromosome_aneuploidy.isnull(),
        "has_age": ds.sample_age_at_recruitment.notnull(),
        "in_phasing_chromosome_x": ds.sample_use_in_phasing_chromosome_x == 1,
        "in_in_phasing_chromosomes_1_22": ds.sample_use_in_phasing_chromosomes_1_22
        == 1,
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


def load_gwas_ds(genotypes_path: str, phenotypes_path: str) -> Dataset:
    ds = load_dataset(genotypes_path, consolidated=True)
    ds = add_covariates(ds)
    ds = add_traits(ds, phenotypes_path)
    ds = ds[[v for v in sorted(ds)]]
    return ds


def wait_fn(attempts, delay):
    delay = min(2 ** attempts * 1000, 300000)
    logger.info(f"Attempt {attempts}, retrying in {delay} ms")
    return delay


def exception_fn(e):
    logger.error(f"A retriable error occurred: {e}")
    logger.error("Traceback:\n")
    logger.error("\n" + "".join(traceback.format_tb(e.__traceback__)))
    return True


@retry(retry_on_exception=exception_fn, wait_func=wait_fn)
def run_trait_gwas(
    ds: Dataset,
    trait_group_id: int,
    trait_name: str,
    min_samples: int,
    retries: int = 3,
) -> pd.DataFrame:
    assert ds["sample_trait_group_id"].to_series().nunique() == 1
    assert ds["sample_trait_name"].to_series().nunique() == 1

    # Filter to complete cases
    start = time.perf_counter()
    n = ds.dims["samples"]
    ds = ds.isel(samples=ds["sample_trait"].notnull().all(dim="traits").values)
    stop = time.perf_counter()
    sample_size = ds.dims["samples"]
    logger.info(
        f"Found {sample_size} complete cases of {n} for '{trait_name}' (id={trait_group_id}) in {stop - start:.1f} seconds"
    )

    # Bypass if sample size too small
    if sample_size < min_samples:
        logger.warning(
            f"Sample size ({sample_size}) too small (<{min_samples}) for trait '{trait_name}' (id={trait_group_id})"
        )
        return None

    logger.info(
        f"Running GWAS for '{trait_name}' (id={trait_group_id}) with {sample_size} samples, {ds.dims['traits']} traits"
    )

    start = time.perf_counter()
    logger.debug(
        f"Input dataset for trait '{trait_name}' (id={trait_group_id}) GWAS:\n{ds}"
    )

    ds = sg.gwas_linear_regression(
        ds,
        dosage="call_dosage",
        covariates="sample_covariate",
        traits="sample_trait",
        add_intercept=True,
        merge=True,
    )

    # Project and convert to data frame for convenience
    # in downstream analysis/comparisons
    ds = ds[
        [
            "sample_trait_id",
            "sample_trait_name",
            "sample_trait_group_id",
            "sample_trait_code_id",
            "variant_id",
            "variant_contig",
            "variant_contig_name",
            "variant_position",
            "variant_p_value",
            "variant_beta",
        ]
    ]
    ds = ds.compute(retries=retries)
    df = (
        ds.to_dataframe()
        .reset_index()
        .assign(sample_size=sample_size)
        .rename(columns={"traits": "trait_index", "variants": "variant_index"})
    )
    stop = time.perf_counter()
    logger.info(
        f"GWAS for '{trait_name}' (id={trait_group_id}) complete in {stop - start:.1f} seconds"
    )
    return df


@retry(retry_on_exception=exception_fn, wait_func=wait_fn)
def save_gwas_results(df: pd.DataFrame, path: str):
    start = time.perf_counter()
    df.to_parquet(path)
    stop = time.perf_counter()
    logger.info(f"Save to {path} complete in {stop - start:.1f} seconds")


@retry(retry_on_exception=exception_fn, wait_func=wait_fn)
def run_batch_gwas(
    ds: xr.Dataset,
    trait_group_id: str,
    trait_names: np.ndarray,
    batch_size: int,
    min_samples: int,
    sumstats_path: str,
):
    # Determine which individual traits should be regressed as part of this group
    mask = (ds["sample_trait_group_id"] == trait_group_id).values
    index = np.sort(np.argwhere(mask).ravel())
    if len(index) == 0:
        logger.warning(f"Trait group id {trait_group_id} not found in data (skipping)")
        return
    trait_name = trait_names[index][0]

    # Break the traits for this group into batches of some maximum size
    batches = np.array_split(index, np.ceil(len(index) / batch_size))
    if len(batches) > 1:
        logger.info(
            f"Broke {len(index)} traits for '{trait_name}' (id={trait_group_id}) into {len(batches)} batches"
        )
    for batch_index, batch in enumerate(batches):
        path = f"{sumstats_path}_{batch_index:03d}_{trait_group_id}.parquet"
        if fs.exists(path):
            logger.info(
                f"Results for trait '{trait_name}' (id={trait_group_id}) at path {path} already exist (skipping)"
            )
            continue
        dsg = ds.isel(traits=batch)
        df = run_trait_gwas(dsg, trait_group_id, trait_name, min_samples=min_samples)
        if df is None:
            continue
        # Write results for all traits in the batch together so that partitions
        # will have a maximum possible size determined by trait batch size
        # and number of variants in the current contig
        df = df.assign(batch_index=batch_index, batch_size=len(batch))
        logger.info(
            f"Saving results for trait '{trait_name}' id={trait_group_id}, batch={batch_index} to path {path}"
        )
        save_gwas_results(df, path)


def run_gwas(
    genotypes_path: str,
    phenotypes_path: str,
    sumstats_path: str,
    variables_path: str,
    batch_size: int = 100,
    trait_group_ids: Optional[Union[Sequence[Union[str, int]], str]] = None,
    min_samples: int = 100,
):
    init()

    logger.info(
        f"Running GWAS (genotypes_path={genotypes_path}, phenotypes_path={phenotypes_path}, "
        f"sumstats_path={sumstats_path}, variables_path={variables_path})"
    )

    ds = load_gwas_ds(genotypes_path, phenotypes_path)

    # Promote to f4 to avoid:
    # TypeError: array type float16 is unsupported in linalg
    ds["call_dosage"] = ds["call_dosage"].astype("float32")

    # Rechunk dosage (from 5216 x 5792 @ TOW) down to something smaller in the
    # variants dimension since variant_chunk x n_sample arrays need to
    # fit in memory for linear regression (652 * 365941 * 4 = 954MB)
    # See: https://github.com/pystatgen/sgkit/issues/390
    ds["call_dosage"] = ds["call_dosage"].chunk(chunks=(652, 5792))

    logger.info(f"Loaded dataset:\n{ds}")

    # Determine the UKB field ids corresponding to all phenotypes to be used
    # * a `trait_group_id` is equivalent to a UKB field id
    if trait_group_ids is None:
        # Use all known traits
        trait_group_ids = list(map(int, np.unique(ds["sample_trait_group_id"].values)))
    elif isinstance(trait_group_ids, str):
        # Load from file
        trait_group_ids = [
            int(v) for v in pd.read_csv(trait_group_ids, sep="\t")["trait_group_id"]
        ]
    else:
        # Assume a sequence was provided
        trait_group_ids = [int(v) for v in trait_group_ids]
    logger.info(
        f"Using {len(trait_group_ids)} trait groups; first 10: {trait_group_ids[:10]}"
    )

    # Loop through the trait groups and run separate regressions for each.
    # Note that the majority of groups (89%) have only one phenotype/trait
    # associated, some (10%) have between 1 and 10 phenotypes and ~1% have
    # large numbers of phenotypes (into the hundreds or thousands).
    trait_names = ds["sample_trait_name"].values
    for trait_group_id in trait_group_ids:
        run_batch_gwas(
            ds=ds,
            trait_group_id=trait_group_id,
            trait_names=trait_names,
            batch_size=batch_size,
            min_samples=min_samples,
            sumstats_path=sumstats_path,
        )
    logger.info("Sumstat generation complete")

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
            "sample_trait_group_id",
            "sample_trait_code_id",
            "sample_trait_name",
        ]
    ]
    ds = ds.chunk("auto")
    path = variables_path + "_variables.zarr"
    logger.info(f"Saving GWAS variables to {path}:\n{ds}")
    save_dataset(ds, path)

    logger.info("Done")


if __name__ == "__main__":
    fire.Fire()
