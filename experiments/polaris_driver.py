'''
Calls Polaris query executors to run requested queries. Records execution time.
'''

# Import executors

# Load query csv

# Do this three times:
    # Perform each query 

    # Record query execution time

    # Save time back to csv

# Take average of times and save avg to csv as `execution_time`

import time
import pandas as pd
import os 
import sys

# main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
main_dir = "/home/uribe055/experiments"
# print(main_dir)
sys.path.append(os.path.join(main_dir, "executors"))

# impact of spatial resolution

t_res = ["hour", "day", "month", "year"]
s_res = [0.25, 0.5, 1.0]
# sys_list = ["TDB"]
sys_list = ["Polaris"]

df_query = pd.read_csv(os.path.join(main_dir, f"queries/changing_resolutions.csv"))

results_list = []

for cur_sys in sys_list:

    # load executors
    if cur_sys == "Polaris":
        from proposed.query_executor_get_raster import GetRasterExecutor as GRExecutor
        from proposed.query_executor_heatmap import HeatmapExecutor as HExecutor
        from proposed.query_executor_find_time2 import FindTimeExecutor as FExecutor
    # elif cur_sys == "Vanilla":
    #     from vanilla.vanilla_get_raster_executor import VanillaGetRasterExecutor as GRExecutor
    #     from vanilla.vanilla_get_heatmap_executor import VanillaGetHeatmapExecutor as HExecutor
    #     from vanilla.vanilla_find_time_executor import VanillaFindTimeExecutor as FExecutor
    # elif cur_sys == "TDB":   # s == "tiledb"
    #     from tiledb2.tiledb_get_raster_executor import tiledb_get_raster_executor as GRExecutor
    #     from tiledb2.tiledb_get_heatmap_executor import tiledb_get_heatmap_executor as HExecutor
    #     from tiledb2.tiledb_find_time_executor import tiledb_find_time_executor as FExecutor
    else:
        print("Unknown system")
        exit


    for t in t_res:
        for s in s_res:

            for q in df_query.to_records():

                qe = GRExecutor(
                variable=q["variable"],
                start_datetime=q["start_time"],
                end_datetime=q["end_time"],
                max_lat=q["max_lat"],
                min_lat=q["min_lat"],
                min_lon=q["min_lon"],
                max_lon=q["max_lon"],
                spatial_resolution=s,
                temporal_resolution=t,
                aggregation=q["aggregation"],
                )
                try:
                    tr = qe.execute()
                    print(f"s: {s}  t: {t}, q: {q}")
                    print(tr)
                except Exception as e:
                    print(q)
                    print(e)
                    tr = -1

                if tr != -1:
                    if cur_sys == "Polaris":
                        ta = 0
                    else:
                        ta = qe.agg()
                    print(f"total time: {tr+ta}")
                    results_list.append({"sys": cur_sys, 
                                         "t_res": t,
                                         "s_res": s,
                                         "tr": tr,
                                         "ta": ta,
                                         "total_time": tr + ta})
                    print("======================\n")
                else:
                    print(f"-1")

results_df = pd.DataFrame(results_list)
out_file = os.path.join(main_dir, f"results/changing_resolutions_{cur_sys}.csv")
results_df.to_csv(out_file, index=False)
