import logging
import logging.config
import os
import warnings
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

warnings.filterwarnings("error")

from dask.diagnostics import ProgressBar
from dask.distributed import Client  # , get_task_stream, performance_report
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
    with warnings.catch_warnings():
        # Ignore this performance warning since rechunking/reorder the phenotype data is not a big scalability issue
        warnings.filterwarnings(
            "ignore",
            message="Slicing with an out-of-order index is generating .* times more chunks",
        )
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


def run_trait_gwas(ds: Dataset, trait_group_id: int, trait_name: str) -> pd.DataFrame:
    assert ds["sample_trait_group_id"].to_series().nunique() == 1
    assert ds["sample_trait_name"].to_series().nunique() == 1

    # Filter to complete cases
    ds = ds.isel(samples=ds["sample_trait"].notnull().all(dim="traits").values)
    sample_size = ds.dims["samples"]

    logger.info(
        f"Running GWAS for trait '{trait_name}' (id={trait_group_id}) with {sample_size} samples, {ds.dims['traits']} phenotypes"
    )

    # ds = ds.isel(variants=slice(10), samples=slice(100)) # TODO: REMOVE
    logger.info(f"GWAS dataset:\n{ds}")

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
    df = (
        ds[
            [
                "sample_trait_id",
                "sample_trait_name",
                "sample_trait_group_id",
                "sample_trait_code_id",
                "variant_id",
                "variant_contig",
                "variant_contig_name",
                "variant_p_value",
                "variant_beta",
            ]
        ]
        # This is necessary prior to `to_dask_dataframe`
        # .unify_chunks().to_dask_dataframe()
        .to_dataframe()
        .reset_index()
        .assign(sample_size=sample_size)
        .rename(columns={"traits": "trait_index", "variants": "variant_index"})
    )
    return df


def run_gwas(
    genotypes_path: str,
    phenotypes_path: str,
    output_path: str,
    batch_size: int = 100,
    trait_group_ids: Optional[Sequence[Union[str, int]]] = None,
):
    init()

    logger.info(
        f"Running GWAS (genotypes_path={genotypes_path}, phenotypes_path={phenotypes_path}, output_path={output_path})"
    )

    ds = load_gwas_ds(genotypes_path, phenotypes_path)

    # Promote to f4 to avoid:
    # TypeError: array type float16 is unsupported in linalg
    ds["call_dosage"] = ds["call_dosage"].astype("float32")

    # Rechunk dosage (from 5216 x 5792 @ TOW) down to something smaller in the
    # variants dimension since variant_chunk x n_sample arrays need to
    # fit in memory for linear regression (652 * 365941 * 4 = 954MB)
    ds["call_dosage"] = ds["call_dosage"].chunk(chunks=(652, 5792))

    logger.info(f"Loaded dataset:\n{ds}")

    # Determine the UKB field ids corresponding to all phenotypes to be used
    # * a `trait_group_id` is equivalent to a UKB field id
    if trait_group_ids is None:
        trait_group_ids = list(map(int, np.unique(ds["sample_trait_group_id"].values)))
    else:
        trait_group_ids = [int(v) for v in trait_group_ids]
    logger.info(f"Using {len(trait_group_ids)} phenotype groups")

    # Loop through the trait groups and run separate regressions for each.
    # Note that the majority of groups (89%) have only one phenotype/trait
    # associated, some (10%) have between 1 and 10 phenotypes and ~1% have
    # large numbers of phenotypes (into the hundreds or thousands).
    trait_names = ds["sample_trait_name"].values
    for i, trait_group_id in enumerate(trait_group_ids):
        # Determine which individual phenotypes should be regressed as part of this group
        mask = (ds["sample_trait_group_id"] == trait_group_id).values
        index = np.sort(np.argwhere(mask).ravel())
        trait_name = trait_names[index][0]

        # Break into batches due to some trait groups having >1000 phenotypes
        batches = np.array_split(index, np.ceil(len(index) / batch_size))
        logger.info(
            f"Generated {len(batches)} batch(es) for trait '{trait_name}' (id={trait_group_id})"
        )
        results = []
        for j, batch in enumerate(batches):
            dsg = ds.isel(traits=batch)
            df = run_trait_gwas(dsg, trait_group_id, trait_name)
            df = df.assign(batch_index=j, batch_size=len(batch))
            results.append(df)

        # Concatenate and save all results for this group
        df = pd.concat(results)
        path = f"{output_path}/sumstats-{trait_group_id}.parquet"
        logger.info(
            f"Saving results for trait '{trait_name}' (id={trait_group_id}) to path {path}"
        )
        df.to_parquet(path)

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
    variables_path = output_path + "/variables.zarr"
    logger.info(f"Saving GWAS variables to {variables_path}:\n{ds}")
    save_dataset(ds, variables_path)

    logger.info("Done")


if __name__ == "__main__":
    fire.Fire()
