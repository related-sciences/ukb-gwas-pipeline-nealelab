from dask.distributed import Client
import gcsfs
import fire
import sgkit
import xarray as xr
import pandas as pd
import numpy as np
import dask.array as da
from sgkit.stats import association
from xarray import Dataset, DataArray
from sgkit_bgen.bgen_reader import unpack_variables
from dask.diagnostics import ProgressBar
import logging
import logging.config
from pathlib import Path

fs = gcsfs.GCSFileSystem()

logging.config.fileConfig(Path(__file__).resolve().parents[1] / "log.ini")
logger = logging.getLogger(__name__)


def load_genotype_ds(genotypes_path: str) -> Dataset:
    store = gcsfs.mapping.GCSMap(genotypes_path, gcs=fs, check=True, create=False)
    ds = xr.open_zarr(store, consolidated=True)
    ds = unpack_variables(ds, dtype='float16')

    # Workaround for https://github.com/pydata/xarray/issues/4386
    ds['call_genotype_probability_mask'] = ds['call_genotype_probability_mask'].astype(bool)
    return ds


def load_sample_qc(sample_qc_path: str) -> Dataset:
    store = gcsfs.mapping.GCSMap(sample_qc_path, gcs=fs, check=True, create=False)
    ds = xr.open_zarr(store, consolidated=True)
    ds = ds.rename_vars(dict(eid='id'))
    ds = ds.rename_vars({v: f'sample_{v}' for v in ds})
    if 'sample_sex' in ds:
        # Rename to avoid conflict with bgen field
        ds = ds.rename_vars({'sample_sex': 'sample_qc_sex'})
    return ds


def apply_sample_qc(ds: Dataset) -> Dataset:
    # See: https://github.com/Nealelab/UK_Biobank_GWAS#imputed-v3-sample-qc
    filters = {
        'no_aneuploidy': ds.sample_sex_chromosome_aneuploidy.isnull(),
        'in_pca': ds.sample_used_in_genetic_principal_components == 1,
        # 1001 = White/British, 1002 = Mixed/Irish
        'in_ethnic_groups': ds.sample_ethnic_background.isin([1001, 1002])
    }
    logger.info('Sample QC filter summary:')
    for k, v in filters.items():
        logger.info(f'\t{k}: {v.to_series().value_counts().to_dict()}')
    mask = np.stack([v.values for v in filters.values()], axis=1)
    mask = np.all(mask, axis=1)
    assert len(mask) == ds.dims['samples']
    logger.info(f'\toverall: {pd.Series(mask).value_counts().to_dict()}')
    return ds.sel(samples=mask)


SAMPLE_QC_COLS = [
    'sample_id',
    'sample_qc_sex',
    'sample_genetic_sex',
    'sample_age_at_recruitment',
    'sample_principal_component',
    'sample_ethnic_background',
    'sample_genotype_measurement_batch',
    'sample_genotype_measurement_plate',
    'sample_genotype_measurement_well'
]


def sample_qc(ds: Dataset, sample_qc_path: str) -> Dataset:
    ds_sqc = load_sample_qc(sample_qc_path)
    ds_sqc = apply_sample_qc(ds_sqc)
    ds_sqc = ds_sqc[SAMPLE_QC_COLS]
    ds = (
        ds.assign_coords(samples=lambda ds: ds.sample_id).merge(
            ds_sqc.assign_coords(samples=lambda ds: ds.sample_id).compute(),
            join='inner',
            compat='override'
        )
    )
    return ds.reset_index('samples').reset_coords(drop=True)


def variant_genotype_counts(ds: Dataset) -> DataArray:
    gti = ds['call_genotype_probability'].argmax(dim='genotypes')
    gti = gti.astype('uint8').expand_dims('genotypes', axis=-1)
    gti = gti == da.arange(ds.dims['genotypes'], dtype='uint8')
    return gti.sum(dim='samples', dtype='uint32')


def variant_qc(ds):
    # Order: het, hom_ref, hom_alt
    ds['variant_genotype_counts'] = variant_genotype_counts(ds)[:, [1, 0, 2]]
    # Hack to add ploidy dim
    # TODO: allow ploidy to be passed in
    ds['dummy'] = xr.DataArray(da.empty((ds.dims['variants'], ds.dims['samples'], 2)),
                               dims=('variants', 'samples', 'ploidy'))
    ds['variant_hwe_p_value'] = sgkit.hardy_weinberg_test(ds, genotype_counts='variant_genotype_counts')[
        'variant_hwe_p_value']
    ds = ds.drop_vars('dummy')

    # Add stdev to filter on as well
    ds['variant_dosage_std'] = ds['call_dosage'].astype('float32').std(dim='samples')
    return ds


TRAIT_ID_COLS = [
    '50',  # Height (https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=50)
    '23098'  # Weight (https://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=23098)
]


def load_traits(phenotypes_path: str):
    df = pd.read_csv(phenotypes_path, sep='\t')
    ds = (
        df[['userId']].rename(columns={'userId': 'id'})
            .rename_axis('samples', axis='rows')
            .to_xarray().drop('samples')
    )
    ds['trait'] = xr.DataArray(
        df[TRAIT_ID_COLS].values,
        dims=('samples', 'traits')
    )
    # TODO: Decide how to partition phenotypes based on presence, or process them individually
    ds['trait_imputed'] = ds.trait.pipe(lambda x: x.where(x.notnull(), x.mean(dim='samples')))
    ds['trait_names'] = xr.DataArray(
        np.array(['height', 'weight'], dtype='S'),
        dims=['traits']
    )
    ds = ds.rename_vars({v: f'sample_{v}' for v in ds})
    return ds


def add_traits(ds: Dataset, phenotypes_path: str) -> Dataset:
    ds_tr = load_traits(phenotypes_path)
    ds = (
        ds.assign_coords(samples=lambda ds: ds.sample_id).merge(
            ds_tr.assign_coords(samples=lambda ds: ds.sample_id),
            join='left',
            compat='override'
        )
    )
    return ds.reset_index('samples').reset_coords(drop=True)


def add_covariates(ds: Dataset, npc: int = 10) -> Dataset:
    covariates = np.column_stack((
        ds['sample_genetic_sex'],
        ds['sample_age_at_recruitment'],
        ds['sample_principal_component'][:, :npc]
    ))
    assert np.all(np.isfinite(covariates))
    ds['sample_covariate'] = xr.DataArray(covariates, dims=('samples', 'covariates'))
    ds['sample_covariate'] = ds.sample_covariate.pipe(
        lambda x: (x - x.mean(dim='samples')) / x.std(dim='samples')
    )
    assert np.all(np.isfinite(ds.sample_covariate))
    return ds


def apply_variant_qc(ds: Dataset) -> Dataset:
    # See: https://github.com/Nealelab/UK_Biobank_GWAS#imputed-v3-variant-qc
    filters = {
        'in_hwe': ds.variant_hwe_p_value > 1e-10,
        'nonzero_stddev': ds.variant_dosage_std > 0,
        'high_info': ds.variant_info > .8,
        # TODO: special case coding variant threshold
        'high_maf': ds.variant_maf > 0.001,
    }
    logger.info('Variant QC filter summary:')
    for k, v in filters.items():
        logger.info(f'\t{k}: {v.to_series().value_counts().to_dict()}')
    mask = np.stack([v.values for v in filters.values()], axis=1)
    mask = np.all(mask, axis=1)
    assert len(mask) == ds.dims['variants']
    logger.info(f'\toverall: {pd.Series(mask).value_counts().to_dict()}')
    return ds.sel(variants=mask)


def load_gwas_ds(genotypes_path: str, sample_qc_path: str, phenotypes_path: str) -> Dataset:
    ds = load_genotype_ds(genotypes_path)
    ds = sample_qc(ds, sample_qc_path)
    ds = variant_qc(ds)
    ds = add_covariates(ds)
    ds = add_traits(ds, phenotypes_path)
    ds = ds[[v for v in sorted(ds)]]
    return ds


def run_gwas():
    # Hard code paths for now
    genotypes_path = 'rs-ukb/prep-data/gt-imputation/ukb_chr21.zarr'
    sample_qc_path = 'rs-ukb/prep-data/main/ukb_sample_qc.zarr'
    phenotypes_path = 'gs://rs-ukb/prep-data/main/phenotypes.v01.subset01.1.tsv'
    output_path = '/home/eczech/data/rs-ukb-local/gwas/cache/chr21_variant_p_value.n5'

    logger.info('Running GWAS')
    client = Client()
    logger.info(f'Dask client:\n{client}')
    ds = load_gwas_ds(genotypes_path, sample_qc_path, phenotypes_path)
    logger.info(f'Loaded dataset:\n{ds}')

    logger.info('Computing dosage standard deviation')
    with ProgressBar():
        ds['variant_dosage_std'] = ds['variant_dosage_std'].compute()

    logger.info('Computing dosage HWE')
    with ProgressBar():
        ds['variant_hwe_p_value'] = ds['variant_hwe_p_value'].compute()

    ds_qc = apply_variant_qc(ds)

    logger.info(f'Running regression')
    with ProgressBar():
        ds_gwas = association.gwas_linear_regression(
            # Promote to f4 to avoid:
            # TypeError: array type float16 is unsupported in linalg
            ds_qc.assign(call_dosage=lambda ds: ds_qc.call_dosage.astype('float32')),
            dosage='call_dosage',
            covariates='sample_covariate',
            traits='sample_trait_imputed',
            add_intercept=True
        )

    logger.info(f'Saving p-values to {output_path}')
    ds_gwas.variant_p_value.to_netcdf(output_path)
    logger.info('Done')


if __name__ == "__main__":
    fire.Fire()


# * Minimal example for stddev benchmarking * #
# import gcsfs
# import xarray as xr
# from sgkit_bgen.bgen_reader import unpack_variables
# from dask.diagnostics import ProgressBar, ResourceProfiler
#
# store = gcsfs.mapping.GCSMap('rs-ukb/prep-data/gt-imputation/ukb_chr21.zarr', gcs=fs, check=True, create=False)
# ds = xr.open_zarr(store, consolidated=True)
# ds = unpack_variables(ds, dtype='float16')
# with ProgressBar(), Profiler() as prof, ResourceProfiler() as rprof:
#     ds['variant_dosage_std'] = ds['variant_dosage_std'].compute()
