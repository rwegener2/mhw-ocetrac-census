"""
This script runs a subsection of MUR data through Ocetrac.
Specifically it:
Sets up a dask cluster.
Creates a _ year climatology of MUR.
Calculates 1 year of anomaly.
Runs 1 year through Ocetrac.
"""
import time
import json

import fsspec
import xarray as xr
from dask.distributed import Client


def main():
    starttime = time.time()
    # Load MUR from Pangeo
    file_location = 's3://mur-sst/zarr'
    ikey = fsspec.get_mapper(file_location, anon=True)

    mur_full = xr.open_zarr(ikey,consolidated=True)
    mur = mur_full['analysed_sst']
    print('loaded dataset')
    executationtime = time.time() - starttime
    print(executationtime, ' seconds')

    # Subset to region (~4 chunks)
    mur_subset = mur.sel(lat=slice(32, 32.5), lon=slice(121.4, 122.2))

    # Calculate climatology array
    climatology = mur_subset.groupby(mur_subset.time.dt.month).mean()

    # Calculate anomaly array for 2018
    mur_2018_subset = mur_subset.sel(time='2018')
    anomaly = mur_2018_subset.groupby(mur_2018_subset.time.dt.month) - climatology
    anomaly = anomaly.load()
    print('calculated anomaly')
    executationtime = time.time() - starttime
    print(executationtime, ' seconds')

    # Calculate threshold values for the climatology
    percentile = 0.9
    threshold = mur_subset.groupby(mur_subset.time.dt.month).quantile(percentile, 
                                                                                dim='time', 
                                                                                keep_attrs=True, 
                                                                                skipna=True,
                                                                            )
    # Create the hot water mask
    hot_water = anomaly.where(
        mur_2018_subset.groupby(mur_2018_subset.time.dt.month)>threshold
    )
    hot_water = hot_water.load()
    print('calculated hot water')
    executationtime = time.time() - starttime
    print(executationtime, ' seconds')

    # Create land mask
    mur_subset_time0 = mur_2018_subset.isel(time=0)
    mask = xr.where(mur_subset_time0 <= 270, 0, 1)
    mask = mask.load()
    print('calculated land mask')

    # Ocetrac
    # Formatting
    hot_water = hot_water.rename({'lon':'x', 'lat':'y'})
    print('renamed vars')

    # Save data arrays
    # hot_water
    hot_water.to_dataset().to_zarr(
        env_vars['hot_water_path'], 
        mode='w',
        consolidated=True
        )

    # land_mask
    mask.to_dataset(name='land_mask').to_zarr(
        env_vars['land_mask_path'], 
        mode='w'
        )
    print('saved datasets')


if __name__ == '__main__':
    # Load environment variables
    with open('./env_vars.json') as f:
        env_vars = json.load(f)
    # Set up Dask clusters
    client = Client()
    print(client.dashboard_link)
    input('proceed?')
    main()
