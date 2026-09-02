import os
import pandas as pd
import sys
from datetime import datetime
sys.path.append(os.environ['HOME']+'/weather/code')
if os.path.exists('/home/yuval'):
    os.chdir('/home/yuval/weather')
from weather import rain_1h, update_stations, update_activity, round_data
import numpy as np
update_stations()
# update_activity()
#get current year

current_year = datetime.now().year
y = str(current_year)
m = str(datetime.now().month).zfill(2)
d = str(datetime.now().day).zfill(2)
opcsv = f'data/rain_{y}.csv'
if not os.path.exists(opcsv):
    df_rain = rain_1h(from_date=f'{y}-01-01', to_date=f'{y}-{m}-{d}', save_csv=opcsv)
else:
    df_sta = pd.read_csv('data/ims_stations.csv')
    df_act = pd.read_csv('data/ims_activity.csv')
    df_rain = pd.read_csv(opcsv)
    latest = df_act['latest'].max()[:10]
    not_in_act_list = [sta for sta in df_sta['stationId'].tolist() if sta not in df_act['stationId'].tolist()]
    if len(not_in_act_list) > 0:
        print(f'Warning: Stations {not_in_act_list} are not in activity list (may be discontinued). Skipping.')
    still_active = df_act[df_act['latest'] >= latest]['stationId'].tolist()
    for ista in range(len(still_active)):
        idsta = still_active[ista]
        sta = df_sta['name'][df_sta['stationId'] == idsta].values[0]
        if sta in df_rain.columns:
            last = np.where(~df_rain[sta].isna())[0]
            if len(last) == 0:  # no rain yet, maybe no successful read yet
                last = df_rain['datetime'][0][:10]
            else:
                last = df_rain['datetime'][last[-1]][:10]
            df_rain_new = rain_1h(stations=[sta], from_date=last, to_date=f'{y}-{m}-{d}', save_csv=False)
            if df_rain_new['datetime'].iloc[-1] > df_rain['datetime'].iloc[-1]:
                istart = np.where(df_rain_new['datetime'] == df_rain['datetime'].iloc[-1])[0][0] + 1
                for irow in range(istart, len(df_rain_new)):
                    df_rain.at[len(df_rain), 'datetime'] = df_rain_new.at[irow, 'datetime']
            if sta in df_rain_new.columns and df_rain_new[sta].notna().any():
                for row_new in np.where(~df_rain_new[sta].isna())[0]:
                    row = np.where(df_rain['datetime'] == df_rain_new.at[row_new, 'datetime'])[0]
                    if len(row) == 0:
                        raise ValueError('Datetime mismatch when updating rain data')
                    df_rain.at[row[0], sta] = df_rain_new.at[row_new, sta]
        else:
            df_rain_new = rain_1h(stations=[sta], from_date=f'{y}-01-01', to_date=f'{y}-{m}-{d}', save_csv=False)
            if df_rain_new['datetime'].iloc[-1] > df_rain['datetime'].iloc[-1]:
                istart = np.where(df_rain_new['datetime'] == df_rain['datetime'].iloc[-1])[0][0] + 1
                for irow in range(istart, len(df_rain_new)):
                    df_rain.at[len(df_rain), 'datetime'] = df_rain_new.at[irow, 'datetime']
            monitor = df_sta['monitors'][df_sta['name'].values == sta].values[0]
            if 'Rain' in monitor:
                df_rain[sta] = np.nan  # maybe someplace in the desert, no rain so far
            if sta in df_rain_new.columns:
                for row_new in np.where(~df_rain_new[sta].isna())[0]:
                    row = np.where(df_rain['datetime'] == df_rain_new.at[row_new, 'datetime'])[0]
                    if len(row) == 0:
                        raise ValueError('Datetime mismatch when updating rain data')
                    df_rain.at[row[0], sta] = df_rain_new.at[row_new, sta]
        print(f'\rUpdated rain data for station {ista+1} of {len(still_active)}: {sta}', end='', flush=True)
print('saving rain update')
df_rain.to_csv(opcsv, index=False)
print('rounding rain data')
round_data(opcsv)
def update_winter(winters, winter_start):
    """Write the Sept-to-Aug totals of one winter into the winters table.

    Adds the row when the winter is not in the table yet, which is what happens
    every September when a new season opens.
    """
    winter = f'{winter_start}-{winter_start + 1}'
    match = winters.index[winters['winter'] == winter]
    if len(match):
        row = match[0]
    else:
        print(f'adding row for winter {winter}')
        row = len(winters)
        winters.at[row, 'winter'] = winter

    parts = []
    for year, start, stop in ((winter_start, f'{winter_start}-09-01', None),
                              (winter_start + 1, None, f'{winter_start + 1}-09-01')):
        path = f'data/rain_{year}.csv'
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path)
        if start is not None:
            df = df[df['datetime'] >= start]
        if stop is not None:
            df = df[df['datetime'] < stop]
        parts.append(df)
    if not parts:
        return winters

    df_combined = pd.concat(parts)
    for station in df_combined.columns[1:]:
        values = df_combined[station].dropna()
        if len(values):  # leave blank rather than 0 when the station recorded nothing
            winters.at[row, station] = float(values.sum())
    return winters


now = datetime.now()
# The winter that is running now, Sept to Aug
current_winter_start = now.year if now.month >= 9 else now.year - 1

winters = pd.read_csv('data/sum_rain_sep_to_aug.csv')
# The previous winter too, so its last days are not lost when the season rolls over
for winter_start in (current_winter_start - 1, current_winter_start):
    winters = update_winter(winters, winter_start)
winters.to_csv('data/sum_rain_sep_to_aug.csv', index=False)
round_data('data/sum_rain_sep_to_aug.csv')
'''TODO: 
- try complete past data
- collect min max temp
'''