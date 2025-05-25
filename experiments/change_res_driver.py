'''
Edited from repo: https://github.com/iharp3/experiment-kit/blob/main/round2/driver.py
    Accessed: May 21, 2025
    
Calls Polaris query executors to run requested queries. Records execution time.

'''


import time
import pandas as pd
import os 
import sys

def experiment_executor(cur_exp, t_res, s_res, df_query):
    results_list = []

    if cur_exp == "GR":
        from executors_find_time.query_executor_get_raster import GetRasterExecutor as Executor
    if cur_exp == "HE":
        from executors_find_time.query_executor_heatmap import HeatmapExecutor as Executor
    if cur_exp == "FT":
        from executors_find_time.query_executor_find_time2 import FindTimeExecutor as Executor

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
            except Exception as e:
                print(f"\nt: {t}\ts: {s}")
                print(e)
                tr = -1

            if tr != -1:
                ta = 0

                # ##### FOR CHANGING RESULT SIZE #####
                # spatial_res = {0:0.25,
                #                1:0.5,
                #                 2:1.0 }
                # ##### ##### ##### ##### ##### #####

                print(f"\n\tT resolution: {t}\n\tS resolution: {s}\n\ttotal time: {tr+ta}")
                results_list.append({"sys": "Polaris-MERRA2", 
                                        "t_res": t,
                                        "s_res": s,
                                        "tr": tr,
                                        "ta": ta,
                                        "total_time": tr + ta,
                                        })
                print("======================\n")
            else:
                sys.exit(1)

    return results_list

if __name__ == "__main__":

    main_dir = "/home/uribe055/merra_2/experiments"
    sys.path.append(os.path.join(main_dir, "executors_find_time"))

    #### For changing resolutions exp (FIGURE 5)#####
    cur_exp = "GR"
    t_resolutions = ["hour", "hour", "hour", "day", "day", "day", "year", "year", "year", "month", "month"]
    # s_resolutions = [0.25, 0.5, 1,0.25, 0.5, 1,0.25, 0.5, 1,0.5, 1]
    s_resolutions = [0, 1, 2, 0, 1, 2, 0, 1, 2, 1,2]
    filename = "changing_resolutions.csv"
    outfilename = "results_" + filename


    ##### MAIN #####
    results_list = experiment_executor(cur_exp=cur_exp,
                                    t_res=t_resolutions,
                                    s_res=s_resolutions,
                                    df_query=pd.read_csv(os.path.join(main_dir, "queries", filename)),
                                    )

    results_df = pd.DataFrame(results_list)
    out_file = os.path.join(main_dir, "results", outfilename)
    results_df.to_csv(out_file, index=False)