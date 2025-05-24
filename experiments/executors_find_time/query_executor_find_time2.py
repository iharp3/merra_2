'''
copied from repo: https://github.com/iharp3/experiment-kit/blob/main/round2/executors/proposed/query_executor_find_time2.py
'''

import pandas as pd
import xarray as xr
import time
import sys

from .query_executor import QueryExecutor
from .query_executor_timeseries import TimeseriesExecutor
from .get_whole_period import get_whole_period_between, get_last_date_of_month, time_array_to_range_ft


class FindTimeExecutor(QueryExecutor):
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
        time_series_aggregation_method: str,  # e.g., "mean", "max", "min"
        filter_predicate: str,  # e.g., ">", "<", "==", "!=", ">=", "<="
        filter_value: float,
        spatial_resolution,  # e.g., 0.25, 0.5, 1.0
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
        self.time_series_aggregation_method = time_series_aggregation_method
        self.filter_predicate = filter_predicate
        self.filter_value = filter_value

    def execute(self):
        # print(f"\t\t\t current executor: PROPOSED FIND TIME")
        if self.temporal_resolution == "hour" and self.filter_predicate != "!=":
            return self._execute_pyramid_hour()
        return self._execute_baseline(self.start_datetime, self.end_datetime)

    def _execute_baseline(self, start_datetime, end_datetime):
        timeseries_executor = TimeseriesExecutor(
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
            self.time_series_aggregation_method,
            metadata=self.metadata.f_path,
        )
        ts = timeseries_executor.execute()
        if self.filter_predicate == ">":
            res = ts.where(ts > self.filter_value, drop=False)
        elif self.filter_predicate == "<":
            res = ts.where(ts < self.filter_value, drop=False)
        elif self.filter_predicate == "==":
            res = ts.where(ts == self.filter_value, drop=False)
        elif self.filter_predicate == "!=":
            res = ts.where(ts != self.filter_value, drop=False)
        elif self.filter_predicate == ">=":
            res = ts.where(ts >= self.filter_value, drop=False)
        elif self.filter_predicate == "<=":
            res = ts.where(ts <= self.filter_value, drop=False)
        else:
            raise ValueError("Invalid filter_predicate")
        res = res.fillna(False)
        res = res.astype(bool)
        return res

    def _execute_pyramid_hour(self):
        """
        Optimizations heuristics:
            - find hour >  x: if year-min >  x, return True ; if year-max <= x, return False
            - find hour <  x: if year-min >= x, return False; if year-max <  x, return True
            - find hour == x: if year-min >  x, return False; if year-max <  x, return False
            - find hour >= x: if year-min >= x, return True ; if year-max <  x, return False
            - find hour <= x: if year-min >  x, return False; if year-max <= x, return True
        """
        years, months, days, hours = get_whole_period_between(self.start_datetime, self.end_datetime)
        # print(f"find time2:\n\tWhole period between:{years, months, days, hours}")
        time_points = pd.date_range(start=self.start_datetime, end=self.end_datetime, freq="3h")
        # print(f"\ttime points:\n{time_points}")
        result = xr.Dataset(
            data_vars={self.variable: (["time"], [None] * len(time_points))},
            coords=dict(time=time_points),
        )
        # print(f"\tresult: \n{result}")
        if years:
            # print("checking years")
            year_range = time_array_to_range_ft(years, "year")
            # print(f"\tyear range: {year_range}")
            year_min, year_max = self._get_min_max_time_series(year_range, "year")
            # print(f"\tyear min/max: {year_min}/{year_max}")
            for year in years:
                year_determined = False
                year_datetime = f"{year}-12-31 00:00:00"
                curr_year_min = year_min[self.variable].sel(time=year_datetime, method="nearest").values.item()
                # print(f"cur_year_min: {curr_year_min}")
                # curr_year_min = year_min[self.variable].sel(time=year_datetime).values.item() #OG
                curr_year_max = year_max[self.variable].sel(time=year_datetime, method="nearest").values.item()
                # print(f"year: {year}, min: {curr_year_min}, max: {curr_year_max}")
                if pd.isnull(curr_year_min):
                    print("cur year is null")
                if self.filter_predicate == ">":
                    if curr_year_min > self.filter_value:
                        # print(f"{year}: min > filter, True")
                        year_determined = True
                        result[self.variable].loc[str(year) : str(year)] = True
                    elif curr_year_max <= self.filter_value:
                        # print(f"{year}: max <= filter, False")
                        year_determined = True
                        result[self.variable].loc[str(year) : str(year)] = False
                elif self.filter_predicate == "<":
                    if curr_year_min >= self.filter_value:
                        # print(f"{year}: min >= filter, False")
                        year_determined = True
                        result[self.variable].loc[str(year) : str(year)] = False
                    elif curr_year_max < self.filter_value:
                        # print(f"{year}: max < filter, True")
                        year_determined = True
                        result[self.variable].loc[str(year) : str(year)] = True
                elif self.filter_predicate == "==":
                    if curr_year_min > self.filter_value or curr_year_max < self.filter_value:
                        # print(f"{year}: min > filter or max < filter, False")
                        year_determined = True
                        result[self.variable].loc[str(year) : str(year)] = False
                if not year_determined:
                    # add months to months
                    months = months + [f"{year}-{month:02d}" for month in range(1, 13)]

        if months:
            # print("checking months")
            month_range = time_array_to_range_ft(months, "month")
            month_min, month_max = self._get_min_max_time_series(month_range, "month")
            for month in months:
                month_determined = False
                month_datetime = f"{month}-{get_last_date_of_month(pd.Timestamp(month))} 00:00:00"
                curr_month_min = month_min[self.variable].sel(time=month_datetime, method="nearest").values.item()
                curr_month_max = month_max[self.variable].sel(time=month_datetime, method="nearest").values.item()
                if self.filter_predicate == ">":
                    if curr_month_min > self.filter_value:
                        # print(f"{month}: min > filter, True")
                        month_determined = True
                        result[self.variable].loc[month:month] = True
                    elif curr_month_max <= self.filter_value:
                        # print(f"{month}: max <= filter, False")
                        month_determined = True
                        result[self.variable].loc[month:month] = False
                elif self.filter_predicate == "<":
                    if curr_month_min >= self.filter_value:
                        # print(f"{month}: min >= filter, False")
                        month_determined = True
                        result[self.variable].loc[month:month] = False
                    elif curr_month_max < self.filter_value:
                        # print(f"{month}: max < filter, True")
                        month_determined = True
                        result[self.variable].loc[month:month] = True
                elif self.filter_predicate == "==":
                    if curr_month_min > self.filter_value or curr_month_max < self.filter_value:
                        # print(f"{month}: min > filter or max < filter, False")
                        month_determined = True
                        result[self.variable].loc[month:month] = False
                if not month_determined:
                    # add days to days
                    days = days + [
                        f"{month}-{day:02d}" for day in range(1, get_last_date_of_month(pd.Timestamp(month)) + 1)
                    ]

        if days:
            # print("checking days")
            day_range = time_array_to_range_ft(days, "day")
            day_min, day_max = self._get_min_max_time_series(day_range, "day")
            for day in days:
                day_determined = False
                day_datetime = f"{day} 00:00:00"
                curr_day_min = day_min[self.variable].sel(time=day_datetime, method="nearest").values.item()
                curr_day_max = day_max[self.variable].sel(time=day_datetime, method="nearest").values.item()
                if self.filter_predicate == ">":
                    if curr_day_min > self.filter_value:
                        # print(f"{day}: min > filter, True")
                        day_determined = True
                        result[self.variable].loc[day:day] = True
                    elif curr_day_max <= self.filter_value:
                        # print(f"{day}: max <= filter, False")
                        day_determined = True
                        result[self.variable].loc[day:day] = False
                elif self.filter_predicate == "<":
                    if curr_day_min >= self.filter_value:
                        # print(f"{day}: min >= filter, False")
                        day_determined = True
                        result[self.variable].loc[day:day] = False
                    elif curr_day_max < self.filter_value:
                        # print(f"{day}: max < filter, True")
                        day_determined = True
                        result[self.variable].loc[day:day] = True
                elif self.filter_predicate == "==":
                    if curr_day_min > self.filter_value or curr_day_max < self.filter_value:
                        # print(f"{day}: min > filter or max < filter, False")
                        day_determined = True
                        result[self.variable].loc[day:day] = False
                if not day_determined:
                    # add hours to hours
                    hours = hours + [f"{day} {hour:02d}:00:00" for hour in range(24)]

        result_undetermined = result["time"].where(result[self.variable].isnull(), drop=True)
        # print(f"\nRESULT UNDETERMINED:\n{result_undetermined}")
        if result_undetermined.size > 0:
            hour_range = time_array_to_range_ft(result_undetermined.values, "hour")
            first_hour = hour_range[0][0]
            last_hour = hour_range[-1][1]
            start = first_hour.strftime("%Y-%m-%d %H:%M:%S")
            end = last_hour.strftime("%Y-%m-%d %H:%M:%S")
            rest = self._execute_baseline(start_datetime=start, end_datetime=end)
            # print(f"\nRESULT[self.variable].sel(time=slice(start,end)):\n")
            # print(result[self.variable].sel(time=slice(start, end)))
            # print(f"\nREST[self.variable]:\n")
            # print(rest[self.variable])

            # Align rest to the time coordinate of the result slice
            aligned_rest = rest[self.variable].reindex(time=result.sel(time=slice(start, end)).time)
            # print(f"\nALIGNED REST :\n{aligned_rest}")
            # Now assignment is safe
            result[self.variable].loc[f"{start}":f"{end}"] = aligned_rest

            # sys.exit(1)
            # print(f"\texecute pyramid hour hour section rest:\n{rest}\nstart/end: {start}/{end}")
            # result[self.variable].loc[f"{start}":f"{end}"] = rest[self.variable]
        result[self.variable] = result[self.variable].astype(bool)
        return result

    def _get_min_max_time_series(self, _range, temporal_res):
        total_start = _range[0][0]
        total_end = _range[-1][1]
        # print(f"\t_range: {_range}")
        # print(f"\ttotal_start: {total_start}")
        # print(f"\ttotal_end: {total_end}")
        min_exec = TimeseriesExecutor(
            variable=self.variable,
            start_datetime=total_start,
            end_datetime=total_end,
            min_lat=self.min_lat,
            max_lat=self.max_lat,
            min_lon=self.min_lon,
            max_lon=self.max_lon,
            temporal_resolution=temporal_res,
            spatial_resolution=self.spatial_resolution,
            aggregation="min",
            time_series_aggregation_method="min",
            metadata=self.metadata.f_path,
        )
        # print(f"\tgenerated min_exec timeseriesexecutor object")
        max_exec = TimeseriesExecutor(
            variable=self.variable,
            start_datetime=total_start,
            end_datetime=total_end,
            min_lat=self.min_lat,
            max_lat=self.max_lat,
            min_lon=self.min_lon,
            max_lon=self.max_lon,
            temporal_resolution=temporal_res,
            spatial_resolution=self.spatial_resolution,
            aggregation="max",
            time_series_aggregation_method="max",
            metadata=self.metadata.f_path,
        )
        min_ts = min_exec.execute()
        max_ts = max_exec.execute()
        return min_ts, max_ts