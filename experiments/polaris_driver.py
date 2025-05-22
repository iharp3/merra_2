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

def experiment_executor(cur_exp, t_res, s_res, df_query):
    results_list = []

    if cur_exp == "GR":
        from executors.query_executor_get_raster import GetRasterExecutor as Executor
    if cur_exp == "HE":
        from executors.query_executor_heatmap import HeatmapExecutor as Executor
    if cur_exp == "FT":
        from executors.query_executor_find_time2 import FindTimeExecutor as Executor

    for t, s in zip(t_res, s_res):
        for q in df_query.to_records():
            qe = Executor(
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
                ta = 0
                print(f"total time: {tr+ta}")
                results_list.append({"sys": "Polaris-Merra2", 
                                        "t_res": t,
                                        "s_res": s,
                                        "tr": tr,
                                        "ta": ta,
                                        "total_time": tr + ta})
                print("======================\n")
            else:
                print(f"-1")

    return results_list

if __name__ == "__main__":
    
    main_dir = "/home/uribe055/experiments"
    sys.path.append(os.path.join(main_dir, "executors"))

    ##### For changing resolutions exp #####
    cur_exp = "GR"
    t_res = ["hour", "day", "month", "year"]
    s_res = [0, 1, 2]
    filename = "changing_resolutions.csv"


    ##### MAIN #####
    results_list = experiment_executor(cur_exp="GR",
                                       t_res=[],
                                       s_res=[],
                                       df_query=pd.read_csv(os.path.join(main_dir, "queries", filename)),
                                       )

    results_df = pd.DataFrame(results_list)
    out_file = os.path.join(main_dir, f"results/changing_resolutions_{"Polaris-Merra2"}.csv")
    results_df.to_csv(out_file, index=False)