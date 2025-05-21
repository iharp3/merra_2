'''
Temporally and spatially aggregate data. 
Edited code from yhuang-db's ERA5 agg driver:
    https://github.com/iharp3/iharp-offline-agg/blob/main/agg_driver.py
    Accessed May 13, 2025
'''

from dask.distributed import LocalCluster
import numpy as np
from numpy import dtype, nan
import xarray as xr
import time as clock
import os
import json
import sys
import pandas as pd


"""
Temporal aggregation for 0.5x0.625-degree (raw-sp):
(raw-sp, 3-hour) -> (raw-sp, day) -> (raw-sp, month) -> (raw-sp, year)
"""
def time_driver():
    cluster = LocalCluster(n_workers=10)
    client = cluster.get_client()

    base_file_name = "AODANA_2015-2024"
    file_name = os.path.join("/home/uribe055/merra_2/data", "AODANA_2015-2024.nc")
    print(f"Processing {base_file_name}\n")
    
    start_time = clock.time()
    ds = xr.open_dataset(file_name, chunks="auto")

    # raw, (day, month, year), mean
    ds_0_day_mean = ds['AODANA'].groupby('time.date', restore_coord_dims=True).mean(dim='time', keep_attrs=True).rename({'date': 'time'}).to_dataset(name="AODANA")
    ds_0_day_mean['time'] = xr.DataArray(pd.to_datetime(ds_0_day_mean['time'].values), dims='time')
    ds_0_day_mean = ds_0_day_mean.compute()
    ds_0_day_mean.to_netcdf(f"{base_file_name}_0_day_mean.nc")
    t1 = clock.time()
    ds_0_month_mean = ds_0_day_mean.resample(time="ME").mean(keep_attrs=True)
    ds_0_month_mean.to_netcdf(f"{base_file_name}_0_month_mean.nc")
    t2 = clock.time()
    ds_0_year_mean = ds_0_month_mean.resample(time="YE").mean(keep_attrs=True)
    ds_0_year_mean.to_netcdf(f"{base_file_name}_0_year_mean.nc")
    t3 = clock.time()

    # 0, (day, month, year), min
    ds_0_day_min = ds["AODANA"].groupby('time.date', restore_coord_dims=True).min(dim='time', keep_attrs=True).rename({'date': 'time'}).to_dataset(name="AODANA")
    ds_0_day_min['time'] = xr.DataArray(pd.to_datetime(ds_0_day_min['time'].values), dims='time')
    ds_0_day_min = ds_0_day_min.compute()
    ds_0_day_min.to_netcdf(f"{base_file_name}_0_day_min.nc")
    t4= clock.time()
    ds_0_month_min = ds_0_day_min.resample(time="ME").min(keep_attrs=True)
    ds_0_month_min.to_netcdf(f"{base_file_name}_0_month_min.nc")
    t5= clock.time()
    ds_0_year_min = ds_0_month_min.resample(time="YE").min(keep_attrs=True)
    ds_0_year_min.to_netcdf(f"{base_file_name}_0_year_min.nc")
    t6= clock.time()

    # 0, (day, month, year), max
    ds_0_day_max = ds["AODANA"].groupby('time.date', restore_coord_dims=True).max(dim='time', keep_attrs=True).rename({'date': 'time'}).to_dataset(name="AODANA")
    ds_0_day_max['time'] = xr.DataArray(pd.to_datetime(ds_0_day_max['time'].values), dims='time')
    ds_0_day_max = ds_0_day_max.compute()
    ds_0_day_max.to_netcdf(f"{base_file_name}_0_day_max.nc")
    t7= clock.time()
    ds_0_month_max = ds_0_day_max.resample(time="ME").max(keep_attrs=True)
    ds_0_month_max.to_netcdf(f"{base_file_name}_0_month_max.nc")
    t8 = clock.time()
    ds_0_year_max = ds_0_month_max.resample(time="YE").max(keep_attrs=True)
    ds_0_year_max.to_netcdf(f"{base_file_name}_0_year_max.nc")
    t9 = clock.time()

    client.close()
    cluster.close()

    td_a = t1 - start_time
    tm_a = t2 - t1
    ty_a = t3 - t2

    td_b = t4 - t3
    tm_b = t5 - t4
    ty_b = t6 - t5

    td_c = t7 - t6
    tm_c = t8 - t9
    ty_c = t9 - t8

    print(f"d: {td_a, td_b, td_c}\nm: {tm_a, tm_b, tm_c}\ny: {ty_a, ty_b, ty_c}")
    agg_time = round(t9 - start_time, 2)
    print(f"TEMPORAL AGG TIME: {agg_time}")

    # td_avg = round((td_a + td_b + td_c)/3, 2)
    # tm_avg = round((tm_a + tm_b + tm_c)/3, 2) 
    # ty_avg = round((ty_a + ty_b + ty_c)/3, 2)

    # temporal_times =  {"temporal_agg_types": ["mean", "min", "max"],
    #                     "temporal_day": [td_a, td_b, td_c],
    #                     "temporal_month": [tm_a, tm_b, tm_c],
    #                     "temporal_year": [ty_a, ty_b, ty_c],
    #                     "temporal_day_avg": td_avg,
    #                     "temporal_month_avg": tm_avg,
    #                     "temporal_year_avg": ty_avg,
    #                     "temporal_total_agg_time": {agg_time}}

    return agg_time
    

"""
Spatial aggregation for daily, monthly, and yearly data:
(0, day) -> (0.5, day)
(0, day) -> (1, day)
(0, month) -> (0.5, month)
(0, month) -> (1, month)
(0, year) -> (0.5, year)
(0, year) -> (1, year)
"""
def space_driver():
    base_file_name = f"AODANA_2015-2024"
    print(f"Processing {base_file_name}")

    start_time = clock.time()
    max_t = 0
    min_t = 0
    mean_t = 0

    for time_res in ["day", "month", "year"]:
        for space in ["1", "2"]:
            if space == "1":
                coarse = 2
            elif space == "2":
                coarse = 4
            else:
                raise ValueError("space must be 1 or 2")

            # max
            ds_time_max = xr.open_dataset(
                f"{base_file_name}_0_{time_res}_max.nc", chunks={"time": 1, "lat": 721, "lon": 1440}
            )
            ds_space_time_max = ds_time_max.coarsen(lat=coarse, lon=coarse, boundary="trim").max()
            ds_space_time_max.to_netcdf(
                f"{base_file_name}_{space}_{time_res}_max.nc", 
            )
            c = clock.time()
            # min
            ds_time_min = xr.open_dataset(
                f"{base_file_name}_0_{time_res}_min.nc", chunks={"time": 1, "lat": 721, "lon": 1440}
            )
            ds_space_time_min = ds_time_min.coarsen(lat=coarse, lon=coarse, boundary="trim").min()
            ds_space_time_min.to_netcdf(
                f"{base_file_name}_{space}_{time_res}_min.nc"
            )
            b = clock.time()
            # mean
            ds_time_mean = xr.open_dataset(
                f"{base_file_name}_0_{time_res}_mean.nc", chunks={"time": 1, "lat": 721, "lon": 1440}
            )
            ds_space_time_mean = ds_time_mean.coarsen(lat=coarse, lon=coarse, boundary="trim").mean()
            ds_space_time_mean.to_netcdf(
                f"{base_file_name}_{space}_{time_res}_mean.nc"
            )
            a = clock.time()

            max_t += round((c - start_time), 2)
            min_t += round((b - c), 2)
            mean_t += round((a - b), 2)

            spatial_times = {f"spatial_{time_res}_{space}_max": round((c - start_time), 2),
                             f"spatial_{time_res}_{space}_min": round((b - c), 2),
                             f"spatial_{time_res}_{space}_mean": round((a - b), 2)}
            
            print(spatial_times)
            
    spatial_times = {"Spatial_max_avg": round(max_t/6, 2),
                    "Spatial_min_avg": round(min_t/6, 2),
                    "Spatial_mean_avg": round(mean_t/6, 2),
                    "Spatial_total_agg_time":round(a - start_time, 2)}
    
    print(spatial_times)

    return round(a - start_time, 2)

"""
Spatial aggregation for hourly data: need to be separated from the above one, as it eats up too much memory
(0, hour) -> (1, hour)
(0, hour) -> (2, hour)
"""
def space2_driver():
    cluster = LocalCluster(n_workers=10)
    client = cluster.get_client()

    base_file_name = f"AODANA_2015-2024"
    print(f"Processing {base_file_name}")
    file_name = os.path.join("/home/uribe055/merra_2/data", "AODANA_2015-2024.nc")
    start_time = clock.time()

    ds = xr.open_dataset(file_name, chunks="auto")
    # 1, hour, mean
    ds_1_hour_mean = ds.coarsen(lat=2, lon=2, boundary="trim").mean()
    ds_1_hour_mean = ds_1_hour_mean.compute()
    ds_1_hour_mean.to_netcdf(f"{base_file_name}_1_hour_mean.nc")

    # 1, hour, max
    ds_1_hour_max = ds.coarsen(lat=2, lon=2, boundary="trim").max()
    ds_1_hour_max = ds_1_hour_max.compute()
    ds_1_hour_max.to_netcdf(f"{base_file_name}_1_hour_max.nc")

    # 1, hour, min
    ds_1_hour_min = ds.coarsen(lat=2, lon=2, boundary="trim").min()
    ds_1_hour_min = ds_1_hour_min.compute()
    ds_1_hour_min.to_netcdf(f"{base_file_name}_1_hour_min.nc")

    # 2, hour, mean
    ds_2_hour_mean = ds.coarsen(lat=4, lon=4, boundary="trim").mean()
    ds_2_hour_mean = ds_2_hour_mean.compute()
    ds_2_hour_mean.to_netcdf(f"{base_file_name}_2_hour_mean.nc")

    # 2, hour, max
    ds_2_hour_max = ds.coarsen(lat=4, lon=4, boundary="trim").max()
    ds_2_hour_max = ds_2_hour_max.compute()
    ds_2_hour_max.to_netcdf(f"{base_file_name}_2_hour_max.nc")

    # 2, hour, min
    ds_2_hour_min = ds.coarsen(lat=4, lon=4, boundary="trim").min()
    ds_2_hour_min = ds_2_hour_min.compute()
    ds_2_hour_min.to_netcdf(f"{base_file_name}_2_hour_min.nc")
    
    end_time = clock.time()

    client.close()
    cluster.close()

    print(round(end_time - start_time, 2))

    return round(end_time - start_time, 2)


if __name__ == "__main__":

    t_t = time_driver()
    print(f"\n~~~Temporal aggregation: {t_t}\n")
    s_t = space_driver()
    print(f"~~~Spatial aggregation: {s_t}\n")
    s_h_t = space2_driver()
    print(f"\tsp.agg. hourly: {s_h_t}\n")

    all_times = {"TOTAL_AGG_TIME": round((t_t+ s_t + s_h_t), 2)}
    print(f"Total Aggregation time:{all_times}")