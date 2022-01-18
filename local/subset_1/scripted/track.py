"""
This script runs Ocetrac on a subsection of MUR data through Ocetrac.
Inputs:
    - `hot_water` dataset
    - `land_mask` dataset
These inputs are saved outputs of `hot_water.py`
"""
import json

import ocetrac
import xarray as xr
from dask.distributed import Client


def main():
    # Open `hot_water`
    hot_water = xr.open_zarr(env_vars['hot_water_path'])
    hot_water = hot_water['analysed_sst']

    # Open `land_mask`
    land_mask = xr.open_zarr(env_vars['land_mask_path'])
    land_mask = land_mask['land_mask']

    # Ocetrac - Run tracker
    Tracker = ocetrac.Tracker(hot_water, land_mask, radius=2, min_size_quartile=0.75, timedim = 'time', xdim = 'x', ydim='y', positive=True)
    blobs = Tracker.track()

    print(blobs)

if __name__ == '__main__':
    # Load environment variables
    with open('./env_vars.json') as f:
        env_vars = json.load(f)
    # Set up Dask cluster
    # dask.config.set(temporary_directory='/homes/metogra/rwegener/tmp/')
    client = Client()
    # print(client.dashboard_link)
    # input('proceed?')
    main()
