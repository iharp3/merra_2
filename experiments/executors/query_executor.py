'''
Edited from github repo: https://github.com/iharp3/experiment-kit/blob/main/round2/executors/proposed/query_executor.py
'''

from abc import ABC, abstractmethod
import xarray as xr

from executors.metadata import Metadata


class QueryExecutor(ABC):
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
        if temporal_resolution == "hour" and spatial_resolution == 0:
            aggregation = None
        # user query parameters
        self.variable = variable
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.min_lat = min_lat
        self.max_lat = max_lat
        self.min_lon = min_lon
        self.max_lon = max_lon
        self.temporal_resolution = temporal_resolution
        self.spatial_resolution = spatial_resolution
        self.aggregation = aggregation

        # query internal variables
        self.variable_short_name = self.variable
        if metadata:
            self.metadata = Metadata(metadata)
        else:
            self.metadata = Metadata("/home/uribe055/merra_2/data_yr_only/metadata_yearly_files_agg_one_file.csv")

    @abstractmethod
    def execute(self) -> xr.Dataset:
        """
        Return: xarray.Dataset, with data variable as loaded-in-memory Numpy array
        """
        pass