# Check data is as expected

import xarray as xr
import os
import sys
import json
from collections import Counter

homeDir = "/home/uribe055/merra_2/data"

# fileNames = [os.path.join(homeDir, f) for f in os.listdir(homeDir) if f.endswith(".nc4")]
# datasets = [xr.open_dataset(f) for f in sorted(fileNames)]

# COMBINE DATASETS
# combined = xr.concat(datasets, dim="time")
# saveName = os.path.join("/home/uribe055/merra_2/data_yr", "2015.nc")

# # SAVE COMBINED DATASETS FILE
# combined.to_netcdf(saveName)

# DELETE INDIVIDUAL FILES
# for f in fileNames:
#     os.remove(f)
#     # print(f)

# READ DATASET
fileName = os.path.join(homeDir, "AODANA_2015-2024_0_month_mean.nc")
ds = xr.open_dataset(fileName)
print(ds)

# CALCULATE YEAR REQUEST TIME
# fileName = os.path.join(homeDir, "runtime_info_2017.jsonl")
# difference = []
# with open(fileName, 'r') as f:
#     for line in f:
#         data = json.loads(line)
#         d = data["total_t"] - data["download_t"]
#         difference.append(d)

# print(f"Number of unique values: {Counter(difference)}")
# print(f"Unique values:\n{difference}")