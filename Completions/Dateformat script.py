# -*- coding: utf-8 -*-
"""
Created on Mon Dec  4 06:11:36 2023

@author: ht185078
"""

import pandas as pd
df = pd.read_csv(r'E:\_Projects\_outputs\completion\completions_jab_hist.csv')

df['ship_date'] = pd.to_datetime(df['ship_date']).dt.date

df['SHIPPED DATE'] = pd.to_datetime(df['SHIPPED DATE']).dt.date

df.to_csv(r'E:\_Projects\_outputs\completion\completions_jab_hist_1.csv', index = False)