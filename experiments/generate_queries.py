'''
Generate queries matching the Polaris queries for ERA5 data.
'''

import csv
import pandas as pd


# GET VARIABLE QUERIES

t_list = ["hour", "day", "year"]
s_list = ["0", "1", "2"]

additional_t = ["month"]
additional_s = ["1", "2"]