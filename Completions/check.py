import pandas as pd
import numpy as np
from datetime import datetime

zeb = pd.read_csv(r'E:\_Projects\_outputs\completion\\completions_jab_hist.csv',encoding='utf-8', low_memory=False, keep_default_na='')
zeb['ship_date'] = pd.to_datetime(zeb['ship_date'],format='mixed')
zeb['ship_date'] = zeb['ship_date'].astype('datetime64[ns]')
zeb.to_csv(r'E:\_Projects\_outputs\completion' + '\\trial.csv',encoding='utf-8', index=False)
