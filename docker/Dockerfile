FROM daskdev/dask:2.30.0

RUN conda install numba==0.51.2 xarray==0.16.1 gcsfs==0.7.1 dask-ml==1.7.0 zarr==2.4.0 pyarrow==2.0.0 -c conda-forge

RUN pip install git+https://github.com/pystatgen/sgkit.git@c5548821653fa2759421668092716d2036834ffe#egg=sgkit