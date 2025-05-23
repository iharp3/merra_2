'''
Edited from github repo: https://github.com/iharp3/experiment-kit/blob/main/round2/executors/proposed/query_executor_get_raster.py
'''

import math
import pandas as pd
import xarray as xr
import time
import random

from executors.query_executor import QueryExecutor

from datetime import datetime, timedelta

class GetRasterExecutor(QueryExecutor):
    def __init__(
        self,
        variable: str,
        start_datetime: str,
        end_datetime: str,
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float,
        temporal_resolution: str,  # e.g., "hour", "day", "month", "year"
        spatial_resolution: float,  # e.g., 0, 1, 2
        aggregation,  # e.g., "mean", "max", "min"
        metadata=None,  # metadata file path
    ):
        super().__init__(
            variable,
            start_datetime,
            end_datetime,
            min_lat,
            max_lat,
            min_lon,
            max_lon,
            temporal_resolution,
            spatial_resolution,
            aggregation,
            metadata=metadata,
        )
        self.ds = None

    def _check_metadata(self):
        """
        Return: [local_files], [api_calls]
        """
        df_overlap, leftover = self.metadata.query_get_overlap_and_leftover(
            self.variable,
            self.start_datetime,
            self.end_datetime,
            self.min_lat,
            self.max_lat,
            self.min_lon,
            self.max_lon,
            self.temporal_resolution,
            self.spatial_resolution,
            self.aggregation,
        )
        assert leftover is None, "Should not have leftover in experiment"

        local_files = df_overlap["file_path"].tolist()
        api_calls = []
        # if leftover is not None: ...
        
        local_files = sorted(local_files)
        return local_files, api_calls

    def execute(self):
        t0 = time.time()
        # 1. check metadata
        file_list, api = self._check_metadata()
        assert len(api) == 0, "Should not call api in experiment"

        # 3.2 read local files
        ds_list = []
        for file in file_list:
            ds = xr.open_dataset(file, engine="netcdf4")
            ds = ds.sortby("time")

            # ds = xr.open_dataset(file, engine="netcdf4").sel(
            #     time=slice(self.start_datetime, self.end_datetime),
            #     lat=slice(self.max_lat, self.min_lat),
            #     lon=slice(self.min_lon, self.max_lon),
            # )
            ds_list.append(ds)

        self.ds = xr.concat([i.chunk() for i in ds_list], dim="time")
        self.ds.compute()

        return time.time() - t0