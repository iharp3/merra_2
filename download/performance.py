'''
Calculate the data request and download time.
'''

import json
import os
import numpy as np

homeDir = "/home/uribe055/merra_2/data_d"

# CALC REQUEST TIME
def get_request_time(filePath):
    total_request_time = 0
    count = 0
    with open(filePath, 'r') as f:
        for line in f:
            count+=1
            l = json.loads(line)
            total_request_time += l[1]

    average_request_time = total_request_time/count

    return total_request_time, average_request_time, count

# CALCULATE DOWNLOAD TIME
def get_download_time(filePathList):
    total_download_time = 0
    count = 0
    yr_count = 0
    for p in filePathList:
        if os.path.exists(p):
            yr_count+=1
            with open(p, 'r') as f:
                for line in f:
                    count+=1
                    l = json.loads(line)
                    total_download_time += l["download_t"]

    average_download_time_yr = total_download_time/yr_count
    average_download_time_day = total_download_time/count

    return total_download_time, average_download_time_yr, average_download_time_day, count

if __name__ == "__main__":

    homeDir = "/home/uribe055/merra_2/data_d"
    fileName = "request_t.jsonl"
    filePathList = [os.path.join(homeDir, f'runtime_info_{y}.jsonl') for y in np.arange(2015, 2025)]

    r_t, r_avg_per_y, n_y = get_request_time(filePath=os.path.join(homeDir, fileName))
    d_t, d_avg_per_y, d_avg_per_d, n_d = get_download_time(filePathList=filePathList)

    total = r_t + d_t

    print(f"\nNumber of years: {n_y}\nNumber of days: {n_d}")

    print("\n\tREQUEST TIME:")
    print(f"\t\tTotal (minutes): {round(r_t/60, 2)}\n\t\tAverage seconds per year: {round(r_avg_per_y, 2)}")
   
    print("\n\tDOWNLOAD TIME:")
    print(f"\t\tTotal (minutes): {round(d_t/60, 2)}\n\t\tAverage seconds per day: {round(d_avg_per_d, 2)}\n\t\tAverage minutes per year: {round(d_avg_per_y/60, 2)}")


    print("\n\tTOTAL DATA AQUISITION TIME:")
    print(f"\t\tseconds: {round((total),2)}\n\t\tminutes: {round(total/60,2)}\n\t\thours: {round((total/60)/60, 2)}")
