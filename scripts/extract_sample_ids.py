import fire
import fsspec
import pandas as pd
import xarray as xr


def get_sample_ids(path):
    ds = xr.open_zarr(fsspec.get_mapper(path))
    return ds.sample_id.to_series().to_list()


def run(input_path, output_path):
    ids = get_sample_ids(input_path)
    ids = pd.DataFrame(dict(sample_id=list(set(ids))))
    ids.to_csv(output_path, sep="\t", index=False)


if __name__ == "__main__":
    fire.Fire()
