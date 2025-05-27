
'''
Get table of average execution time
Execution time	        get variable	find time	heatmap
3-hour, 0.5x0.625			
day, 1 x 1.25			
year, 2 x 2.5	
'''

import pandas as pd
import numpy as np
import os

fp = "/home/uribe055/merra_2/experiments/results"
res = "results_changing_resolutions.csv"
siz = "results_changing_result_size.csv"
hea = "results_heatmap.csv"
fin = "results_find_time.csv"

t_res = ["day", "month"]
s_res = [1, 1]


##### changing resolution #####
# df = pd.read_csv(os.path.join(fp, res))
# for t, s in zip(t_res, s_res):
#     d1 = df[df['t_res'] == t]
#     d2 = d1[d1['s_res'] == s]
#     avg_t = d2['total_time'].mean()
#     print(f'"{t}_{s}":{avg_t},')

##### changing result size #####
# df = pd.read_csv(os.path.join(fp, siz))
# col_name = 'percent_area'
# vals = df[col_name].unique()

##### heatmap #####
# df = pd.read_csv(os.path.join(fp, hea))
# col_name = 'time_span'
# vals = df[col_name].unique()

# ##### find time #####
df = pd.read_csv(os.path.join(fp, fin))
col_name = 'filter_value'
vals = df[col_name].unique()
# t_res = ["hour", "year"]
# s_res = [0, 2]

# print(f"Total times for all result sizes:")
for t, s in zip(t_res, s_res):
    d1 = df[df['t_res'] == t]
    d2 = d1[d1['s_res'] == s]
    avg_t = d2['total_time'].mean()
    print(f'"{t}_{s}":{avg_t},')

# for v in vals:
#     print(f"\nTotal time for {v} {col_name}")
#     d0 = df[df[col_name] == v]
#     for t, s in zip(t_res, s_res):
#         d1 = d0[d0['t_res'] == t]
#         d2 = d1[d1['s_res'] == s]
#         avg_t = d2['total_time'].mean()

#         print(f'"{t}_{s}":{avg_t},')