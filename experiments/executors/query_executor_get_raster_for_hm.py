'''
copied from repo: https://github.com/iharp3/experiment-kit/blob/main/round2/executors/proposed/query_executor_get_raster_for_hm.py
'''

import math
import pandas as pd
import xarray as xr

from .query_executor import QueryExecutor


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
        spatial_resolution: float,  # e.g., 0.25, 0.5, 1.0
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
        if leftover is not None:
            leftover_min_lat = math.floor(leftover.lat.min().item())
            leftover_max_lat = math.ceil(leftover.lat.max().item())
            leftover_min_lon = math.floor(leftover.lon.min().item())
            leftover_max_lon = math.ceil(leftover.lon.max().item())
            leftover_start_datetime = pd.Timestamp(leftover.time.min().item())
            leftover_end_datetime = pd.Timestamp(leftover.time.max().item())
            leftover_start_year, leftover_start_month, leftover_start_day = (
                leftover_start_datetime.year,
                leftover_start_datetime.month,
                leftover_start_datetime.day,
            )
            leftover_end_year, leftover_end_month, leftover_end_day = (
                leftover_end_datetime.year,
                leftover_end_datetime.month,
                leftover_end_datetime.day,
            )

            years = [str(i) for i in range(leftover_start_year, leftover_end_year + 1)]
            months = [str(i).zfill(2) for i in range(1, 13)]
            days = [str(i).zfill(2) for i in range(1, 32)]
            if self.temporal_resolution == "month":
                if leftover_start_year == leftover_end_year:
                    months = [str(i).zfill(2) for i in range(leftover_start_month, leftover_end_month + 1)]
            if self.temporal_resolution == "day" or self.temporal_resolution == "hour":
                if leftover_start_year == leftover_end_year:
                    months = [str(i).zfill(2) for i in range(leftover_start_month, leftover_end_month + 1)]
                    if leftover_start_month == leftover_end_month:
                        days = [str(i).zfill(2) for i in range(leftover_start_day, leftover_end_day + 1)]

            dataset = "reanalysis-era5-single-levels"
            request = {
                "product_type": ["reanalysis"],
                "variable": [self.variable],
                "year": years,
                "month": months,
                "day": days,
                "time": [f"{str(i).zfill(2)}:00" for i in range(0, 24)],
                "data_format": "netcdf",
                "download_format": "unarchived",
                "area": [leftover_max_lat, leftover_min_lon, leftover_min_lat, leftover_max_lon],
            }
            api_calls.append((dataset, request))
        local_files = sorted(local_files)
        # print("local files:", local_files)
        # # print("api:", api_calls)
        return local_files, api_calls

    def execute(self):
        # 1. check metadata
        file_list, api = self._check_metadata()
        assert len(api) == 0, "Should not call api in experiment"

        # 3.2 read local files
        ds_list = []
        for file in file_list:
            ds = xr.open_dataset(file, engine="netcdf4").sel(
                time=slice(self.start_datetime, self.end_datetime),
                lat=slice(self.max_lat, self.min_lat),
                lon=slice(self.min_lon, self.max_lon),
            )
            ds_list.append(ds)

        # 3.3 assemble result
        # compat="override" is a temporal walkaround as pre-aggregation value conflicts with downloaded data
        # future solution: use new encoding when write pre-aggregated data
        # try:
        #     ds = xr.merge([i.chunk() for i in ds_list], compat="no_conflicts")
        # except ValueError:
        #     print("WARNING: conflict in merging data, use override")
        #     ds = xr.merge([i.chunk() for i in ds_list], compat="override")

        ds = xr.concat([i.chunk() for i in ds_list], dim="time")
        return ds.compute()