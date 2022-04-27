##########
# Opening local geopolar from netcdf with mfdataset
##########
import xarray as xr

geopolar = xr.open_mfdataset("/data/pacific/rwegener/noaa-geopolar-nc/*.nc")

##########
# 
##########
