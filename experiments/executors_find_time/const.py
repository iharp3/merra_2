'''
Copied from github repo: https://github.com/iharp3/experiment-kit/blob/main/round2/executors/proposed/utils/const.py
    Accessed: May 2025
'''

import numpy as np
import xarray as xr

ds_raw = xr.Dataset()
ds_raw["lat"] = np.arange(60, 90.1, 0.5)
ds_raw["lon"] = np.arange(-70, -10, 0.625)
ds_1 = ds_raw.coarsen(lat=2, lon=2, boundary="trim").max()
ds_2 = ds_raw.coarsen(lat=4, lon=4, boundary="trim").max()


def get_lat_lon_range(spatial_resolution):
    if spatial_resolution == 0:
        lat_range = ds_raw.lat.values
        lon_range = ds_raw.lon.values
    elif spatial_resolution == 1:
        lat_range = ds_1.lat.values
        lon_range = ds_1.lon.values
    elif spatial_resolution == 2:
        lat_range = ds_2.lat.values
        lon_range = ds_2.lon.values
    else:
        raise ValueError("Invalid spatial_resolution")
    return lat_range, lon_range, lat_range[::-1]


def time_resolution_to_freq(time_resolution):
    if time_resolution == "hour":
        return "h"
    elif time_resolution == "day":
        return "D"
    elif time_resolution == "month":
        return "ME"
    elif time_resolution == "year":
        return "YE"
    else:
        raise ValueError("Invalid time_resolution")