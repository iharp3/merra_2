'''
Duplicate 2015, 2016, and 2017 
    to run 10 year experiments while download doesn't work.
'''

import os
import xarray as xr
import pandas as pd


homeDir = "/home/uribe055/merra_2/data_yr"

leap_df = xr.open_dataset(os.path.join(homeDir, "2016.nc"))
df_1 = xr.open_dataset(os.path.join(homeDir, "2015.nc"))
df_2 = xr.open_dataset(os.path.join(homeDir, "2017.nc"))

def get_new_dates(y, ds):
    return ds.time.to_index().map(lambda dt: dt.replace(year=y))

def save_duplicate_datasets(yr_list, names_list, leap_list):

    count = 0
    for y, n, leap in zip(yr_list, names_list, leap_list):
        if leap:
            new_time = get_new_dates(y, leap_df)
            leap_df['time'] = new_time
            leap_df.to_netcdf(os.path.join(homeDir, n))
        else:
            if count%2==0:
                new_time = get_new_dates(y, df_1)
                df_1['time'] = new_time
                df_1.to_netcdf(os.path.join(homeDir, n))
            else:
                new_time = get_new_dates(y, df_2)
                df_2['time'] = new_time
                df_2.to_netcdf(os.path.join(homeDir, n))
            
            count+=1

if __name__ == '__main__':
    yr_list = [2018, 2019, 2020, 2021, 2022, 2023, 2024]
    names_list = ['2018.nc', '2019.nc', '2020.nc', '2022.nc', '2023.nc', '2024.nc']
    leap_list = [False, False, True, False, False, False, True]

    save_duplicate_datasets(yr_list, names_list, leap_list)