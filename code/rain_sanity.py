import os
import pandas as pd
import sys
from datetime import datetime
sys.path.append(os.environ['HOME']+'/weather/code')
from weather import rain_1h, update_stations
import numpy as np

station = 'METZOKE DRAGOT'
if False:
    winters = []
    for year in range(2015, 2027):
        df = pd.read_csv(f'data/rain_{year}.csv')
        # half2: Jan 1 to Sept 1 of current year
        half2 = np.nansum(df[station][df['datetime'] < f'{year}-09-01'])
        
        if year == 2015:
            df_prev = df
            continue
        
        # half1: Sept 1 of previous year to Jan 1 of current year
        half1 = np.nansum(df_prev[station][df_prev['datetime'] >= f'{year-1}-09-01'])
        
        total = half1 + half2
        winters.append(total)
        df_prev = df

winters = []
for year in range(2015, 2027):
    df = pd.read_csv(f'data/rain_{year}.csv')
    # half2: Jan 1 to Sept 1 of current year
    df2 = df[df['datetime'] < f'{year}-09-01']
    # half2 = np.nansum(df[station][df['datetime'] < f'{year}-09-01'])
    
    if year == 2015:
        df_prev = df
        continue
    
    # half1: Sept 1 of previous year to Jan 1 of current year
    df1 = df_prev[df_prev['datetime'] >= f'{year-1}-09-01']
    # half1 = np.nansum(df_prev[station][df_prev['datetime'] >= f'{year-1}-09-01'])
    df_combined = pd.concat([df1, df2])
    idate = np.where(np.array([md[5:10] for md in df_combined['datetime']]) == '01-03')[0][-1]
    month_day_okay = np.array([False]*len(df_combined))
    month_day_okay[:idate+1] = True
    df_combined = df_combined[month_day_okay]
    total = np.nansum(df_combined[station])
    winters.append(total)
    df_prev = df

print(winters[-1]/np.median(winters[:-1]))


# check Negev 2015-2025
df_stations = pd.read_csv('data/ims_stations.csv')
df_stations = df_stations[df_stations['regionId'] == 12]
fall = np.zeros((11, 4))
for iyear, year in enumerate(range(2015, 2026)):
    df = pd.read_csv(f'data/rain_{year}.csv')
    #filter stations
    keep_columns = [True] + [s in df_stations['name'].tolist() for s in df.columns[1:]]
    df = df.iloc[:, keep_columns]
    for imonth, month in enumerate(range(9, 13)):
        ismonth = np.array([md[5:7] for md in df['datetime']]) == f'{month:02d}'
        df_month = df[ismonth]
        fall[iyear, imonth] = np.median(np.nansum(df_month.values[:, 1:], 0))
winter = np.zeros((11, 8))
for iyear, year in enumerate(range(2016, 2027)):
    df = pd.read_csv(f'data/rain_{year}.csv')
    #filter stations
    keep_columns = [True] + [s in df_stations['name'].tolist() for s in df.columns[1:]]
    df = df.iloc[:, keep_columns]
    for imonth, month in enumerate(range(1, 9)):
        ismonth = np.array([md[5:7] for md in df['datetime']]) == f'{month:02d}'
        df_month = df[ismonth]
        winter[iyear, imonth] = np.median(np.nansum(df_month.values[:, 1:], 0))
fullyear = np.concatenate([fall, winter], axis=1)
plt.figure()
plt.plot(np.median(np.cumsum(fullyear[:-1,:], axis=1), axis=0), label='2015-2025')
plt.plot(np.cumsum(fullyear[-1,:]), label='2026')
plt.xticks(range(12), ['Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug'])
plt.legend()
plt.grid()


plt.figure()
plt.plot(range(12), np.cumsum(fullyear[:-1,:], axis=1).T, 'k')
plt.plot(np.median(np.cumsum(fullyear[:-1,:], axis=1), axis=0), label='median(cumsum)')
plt.plot(np.cumsum(np.median(fullyear[:-1,:], axis=0)), label='cumsum(median)')
plt.plot(np.cumsum(fullyear[-1,:]), label='2026')
plt.xticks(range(12), ['Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug'])
plt.legend()
plt.grid()
for ii in range(11):
    plt.text(12, np.sum(fullyear[ii,:]), str(ii+2015))