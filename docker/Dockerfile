FROM daskdev/dask:2020.12.0

RUN conda install numba==0.52.0 xarray==0.16.2 gcsfs==0.7.1 dask-ml==1.7.0 zarr==2.6.1 pyarrow==2.0.0 -c conda-forge

RUN pip install git+https://github.com/eric-czech/sgkit.git@9323b11ee9e982186f37f001f8047cadc02173c0#egg=sgkit