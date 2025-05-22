'''
copied from repo: https://github.com/iharp3/experiment-kit/blob/main/round2/executors/proposed/query_executor_timeseries.py
'''

from xarray.core.dataset import Dataset
from .query_executor import *
from .query_executor_get_raster_for_hm import GetRasterExecutor


class TimeseriesExecutor(QueryExecutor):
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
        time_series_aggregation_method: str,  # e.g., "mean", "max", "min"
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
        self.time_series_aggregation_method = time_series_aggregation_method

    def execute(self):
        get_raster_executor = GetRasterExecutor(
            variable=self.variable,
            start_datetime=self.start_datetime,
            end_datetime=self.end_datetime,
            min_lat=self.min_lat,
            max_lat=self.max_lat,
            min_lon=self.min_lon,
            max_lon=self.max_lon,
            temporal_resolution=self.temporal_resolution,
            spatial_resolution=2,
            aggregation=self.time_series_aggregation_method,
            metadata=self.metadata.f_path,
        )
        raster = get_raster_executor.execute()
        if self.time_series_aggregation_method == "mean":
            return raster.mean(dim=["lat", "lon"]).compute()
        elif self.time_series_aggregation_method == "max":
            return raster.max(dim=["lat", "lon"]).compute()
        elif self.time_series_aggregation_method == "min":
            return raster.min(dim=["lat", "lon"]).compute()
        else:
            raise ValueError(f"Invalid time series aggregation method: {self.time_series_aggregation_method}")