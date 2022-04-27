import time

import xarray as xr

start_time = time.time()

# Read file
filepath = 'https://ncsa.osn.xsede.org/Pangeo/pangeo-forge/noaa-coastwatch-geopolar-sst-feedstock/noaa-coastwatch-geopolar-sst.zarr'
geopolar = xr.open_zarr(filepath)

# get subset
geopolar = geopolar.analysed_sst.sel(lat=slice(40, 41), lon=slice(-70, -69))

# write
geopolar.to_dataset().to_zarr('./test.zarr')

print('Write took: ', time.time() - start_time)
