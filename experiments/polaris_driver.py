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
            time_series_aggregation_method=q["aggregation"],    # FOR Find Time 
            aggregation=q["aggregation"],
            filter_predicate=q["filter_predicate"],     # FOR Find Time 
            filter_value=q["filter_value"],     # FOR Find Time 
            )
            if cur_exp == "FT":
                # try:
                t0 = time.time()
                qe.execute()
                tr = time.time() - t0
                # except Exception as e:
                    # print("find time query fail")
                    # print(q)
                    # print(e)
                    # tr = -1
            else:        
                try:
                    tr = qe.execute()
                except Exception as e:
                    print(f"\nt: {t}\ts: {s}\tt0: {q["start_time"]}\tt1: {q["end_time"]}")
                    # print(q)
                    print(e)
                    tr = -1

            if tr != -1:
                if cur_exp == "FT":
                    fta = 0
                    print(f"find time total time: {tr + fta}")
                    results_list.append({"sys": "Polaris-MERRA2",
                                                "t_res": t,
                                                "s_res": s,
                                                "filter_value": q["filter_value"],
                                                "tr": tr,
                                                "ta": fta,
                                                "total_time": tr + fta})
                else:
                    if tr != -1:
                        ta = 0
                        print(f"total time: {tr+ta}")
                        results_list.append({"sys": "Polaris-MERRA2", 
                                                "t_res": t,
                                                "s_res": s,
                                                "tr": tr,
                                                "ta": ta,
                                                "total_time": tr + ta,
                                                "percent_area": q["percent_area"]})     # percent_area just for change in result size
                        print("======================\n")
                    else:
                        # print(f"-1")
                        pass
            else:
                pass

    return results_list

if __name__ == "__main__":

    # filename = "/home/uribe055/merra_2/experiments/results/results_changing_result_size.csv"
    # df = pd.read_csv(filename)
    # df = df[['sys','t_res','s_res','tr','ta','total_time','percent_area']]
    # df.to_csv(filename, index=False)

    # sys.exit(1)
    
    all_results = []
    for i in range(1,4):
        main_dir = "/home/uribe055/merra_2/experiments"
        sys.path.append(os.path.join(main_dir, "executors"))

        ##### For changing resolutions exp (FIGURE 5)#####
        # cur_exp = "GR"
        # t_resolutions = ["hour", "hour", "hour", "day", "day", "day", "year", "year", "year", "month", "month"]
        # s_resolutions = [0.25, 0.5, 1,0.25, 0.5, 1,0.25, 0.5, 1,0.5, 1]
        # filename = "changing_resolutions.csv"
        # outfilename = "results_" + filename

        ##### For changing result size exp (FIGURE 6) #####
        # cur_exp = "GR"
        # t_resolutions = ["hour", "hour", "hour", "year", "year", "year",  "month", "month", "month", "hour", "hour", "hour", "year", "year", "year"]
        # s_resolutions = [0.25, 0.25, 0.25,0.25, 0.25, 0.25, 0.5, 0.5, 0.5, 1,1,1, 1,1,1]
        # # percent_area = [1, 25, 50, 1, 25, 50,1, 25, 50,1, 25, 50,1, 25, 50]
        # filename = "changing_result_size.csv"
        # outfilename = "results_" + filename 

        ##### For find time exp (FIGURE 8) #####
        cur_exp = "FT"
        t_resolutions = ["hour"]
        s_resolutions = [1]
        filename = "find_time.csv"
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