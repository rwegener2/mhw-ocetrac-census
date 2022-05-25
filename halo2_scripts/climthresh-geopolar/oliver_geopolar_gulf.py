from datetime import datetime

import numpy as np
import xarray as xr
import dask.config
import dask.array as da
from dask.distributed import Client
import marineHeatWaves as mhw


# Not to future self: use python -u flag with nohup to remove buffering
def main():
   start = datetime.now()
   print('Start! ', start.strftime("%H:%M:%S"))
   print('access data ------')

   filepath = 'https://ncsa.osn.xsede.org/Pangeo/pangeo-forge/noaa-coastwatch-geopolar-sst-feedstock/noaa-coastwatch-geopolar-sst.zarr'
   geopolar = xr.open_zarr(filepath)
   geopolar = geopolar.analysed_sst

   min_lat, max_lat, min_lon, max_lon = (32, 53, -79, -42)
   geopolar = geopolar.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon))

   def format_time(time_np):
      # Format time values
      time_dt_list = [datetime.strptime(str(time), '%Y-%m-%dT%H:%M:%S.000000000') for time in time_np]
      return np.array([time.toordinal() for time in time_dt_list])

   print('preprocessing ---------')

   time_ordinal = format_time(geopolar.time.values)

   sst_np = geopolar.values

   sst_np[np.isnan(sst_np)] = 0

   print('loop --------')

   # Setup data
   time_size = len(geopolar.time)
   data = da.from_array(sst_np, chunks=(-1, 150, 150))
   time = time_ordinal

   # define a wrapper to rearrange arguments
   def func1d_climatology(arr, time):
      _, point_clim = mhw.detect(time, arr)
      # return climatology
      return point_clim['seas']

   # define a wrapper to rearrange arguments
   def func1d_threshold(arr, time):
      _, point_clim = mhw.detect(time, arr)
      # return threshold
      return point_clim['thresh']

   # climatology = da.apply_along_axis(func1d_climatology, 0, data, time=time, dtype=data.dtype, shape=(time_size,))
   threshold = da.apply_along_axis(func1d_threshold, 0, data, time=time, dtype=data.dtype, shape=(time_size,))

   compute_time = datetime.now()
   print('About to compute: ', compute_time.strftime("%H:%M:%S"))

   print('computing climatology/threshold ----- ')

   # climatology.compute()

   compute_time = datetime.now()
   # print('Onto the threshold: ', compute_time.strftime("%H:%M:%S"))

   threshold.compute()

   compute_time = datetime.now()
   print('Compute completed: ', compute_time.strftime("%H:%M:%S"))

   print('saving datasets ------------')

   # clim = xr.DataArray(climatology, coords = geopolar.coords, dims=geopolar.dims)
   thresh = xr.DataArray(threshold, coords = geopolar.coords, dims=geopolar.dims)

   # rename variables
   # clim = clim.rename('climatology')
   thresh = thresh.rename('threshold')

   # add array attributes
   # clim.attrs['comment'] = 'climatology computed using Eric Oliver marineHeatWave package. ' \
   #    'Uses all defaults from .detect() function - 5 day window half width, 31 day smoothing' \

   # Save
   # clim_ds = xr.DataArray(climatology, coords = geopolar.coords, dims=geopolar.dims).to_dataset()
   # clim.to_dataset().to_netcdf('./gulfstream_climatology.nc')

   thresh.attrs['comment'] = '90th percentile treshold values computed using Eric Oliver ' \
       'marineHeatWave package. Uses all defaults from .detect() function - 5 day window' \
       ' half width, 31 day smoothing'
   
   thresh.to_dataset().to_netcdf('./gulfstream_threshold.nc')

   # merge into a single dataset
   # output = xr.merge([clim, thresh])

   # assign global metadata
   # output = output.assign_attrs(geopolar.attrs)

   # add new fields
   # output.attrs['source_dataset'] = 'Coastwatch Geopolar SST'

   # drop obsolute fields from original attributes
   # output.attrs.pop('long_name')
   # output.attrs.pop('reference')
   # output.attrs.pop('comment')

   # output.to_netcdf('../data/gulfstream_climatology_threshold.nc')

   end = datetime.now()
   print('End! ', end.strftime("%H:%M:%S"))

   client.close()

if __name__ == '__main__':
    # Set up Dask cluster
    client = Client()
    dask.config.set(temporary_directory='/data/pacific/rwegener')
    print(client.dashboard_link)
    main()
