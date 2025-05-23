'''
Edited from repo: https://github.com/iharp3/experiment-kit/blob/main/round2/driver.py
    Accessed: May 21, 2025
    
Calls Polaris query executors to run requested queries. Records execution time.

Outline:    
'''


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
                # print(f"s: {s}  t: {t}, q: {q}")
                # print(tr)
            except Exception as e:
                print(f"\nt: {t}\ts: {s}\tt0: {q["start_time"]}\tt1: {q["end_time"]}")
                # print(q)
                print(e)
                tr = -1

            # tr = qe.execute()
            # print(f"s: {s}  t: {t}, q: {q}")
            # print(tr)

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
                # print(f"-1")
                pass

    return results_list

if __name__ == "__main__":
    
    all_results = []
    for i in range(1,4):
        main_dir = "/home/uribe055/merra_2/experiments"
        sys.path.append(os.path.join(main_dir, "executors"))

        ##### For changing resolutions exp (FIGURE 5)#####
        # cur_exp = "GR"
        # t_resolutions = ["hour", "hour", "hour", "day", "day", "day", "year", "year", "year", "month", "month"]
        # s_resolutions = [0, 1, 2, 0, 1, 2, 0, 1, 2, 1, 2]
        # filename = "changing_resolutions.csv"
        # outfilename = "results_" + filename

        ##### For changing result size exp (FIGURE 6) #####
        cur_exp = "GR"
        t_resolutions = ["hour", "hour", "hour", "year", "year", "year",  "month", "month", "month", "hour", "hour", "hour", "year", "year", "year"]
        s_resolutions = [0,0,0, 0,0,0, 1,1,1, 2,2,2, 2,2,2]
        # percent_area = [1, 25, 50, 1, 25, 50,1, 25, 50,1, 25, 50,1, 25, 50]
        filename = "changing_result_size.csv"
        outfilename = "results_" + filename 

    ##### MAIN #####
        results_list = experiment_executor(cur_exp=cur_exp,
                                        t_res=t_resolutions,
                                        s_res=s_resolutions,
                                        df_query=pd.read_csv(os.path.join(main_dir, "queries", filename)),
                                        )
        all_results += results_list
    
    results_df = pd.DataFrame(all_results)
    out_file = os.path.join(main_dir, "results", outfilename)
    results_df.to_csv(out_file, index=False)