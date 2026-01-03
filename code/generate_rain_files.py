# import os
# import pandas as pd
from weather import rain_1h
for year in range(1992, 1980, -1):
    y = str(year)
    df_rain = rain_1h(from_date=f'{y}-01-01', to_date=f'{y}-12-31')
    
