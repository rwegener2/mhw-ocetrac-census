"""
This script runs Ocetrac on a subsection of MUR data through Ocetrac.
Inputs:
    - `hot_water` dataset
    - `land_mask` dataset
These inputs are saved outputs of `hot_water.py`
"""
import json
import time

import ocetrac
import xarray as xr
import dask
from dask.distributed import Client


def main():
    starttime = time.time()
    # Open `hot_water`
    hot_water = xr.open_zarr(env_vars['hot_water_path'])
    hot_water = hot_water['analysed_sst']
    # Fixing error: Dimension y on 0th function argument to apply_ufunc with dask='parallelized' 
    # consists of multiple chunks, but is also a core dimension
    hot_water = hot_water.chunk(dict(lat=-1))
    hot_water = hot_water.chunk(dict(lon=-1))
    
    # Open `land_mask`
    land_mask = xr.open_zarr(env_vars['land_mask_path'])
    land_mask = land_mask['land_mask']

    # Ocetrac - Run tracker
    Tracker = ocetrac.Tracker(hot_water, land_mask, radius=2, min_size_quartile=0.75, timedim = 'time', xdim = 'lon', ydim='lat', positive=True)
    blobs = Tracker.track()

    print(blobs)
    executationtime = time.time() - starttime
    print(executationtime, ' seconds')

if __name__ == '__main__':
    # Load environment variables
    with open('./scripted/env_vars.json') as f:
        env_vars = json.load(f)
    # Set up Dask cluster
    dask.config.set(temporary_directory=env_vars['temp_dir'])
    client = Client(n_workers=4)
    print(client.dashboard_link)
    main()
