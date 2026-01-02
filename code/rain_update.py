import os
import pandas as pd
import sys
from datetime import datetime
sys.path.append(os.environ['HOME']+'/weather/code')
from weather import rain_1h, update_stations
import numpy as np
update_stations()
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
        raise ValueError(f'Stations {not_in_act_list} are not in activity list. Please check ims_activity.csv')
    still_active = df_act[df_act['latest'] >= latest]['stationId'].tolist()
    for ista in range(len(still_active)):
        idsta = still_active[ista]
        sta = df_sta['name'][df_sta['stationId'] == idsta].values[0]
        if sta in df_rain.columns:
            last = df_rain['datetime'][np.where(~df_rain[sta].isna())[0][-1]][:10]
            df_rain_new = rain_1h(stations=[sta], from_date=last, to_date=f'{y}-{m}-{d}', save_csv=False)
            if df_rain_new['datetime'].iloc[-1] > df_rain['datetime'].iloc[-1]:
                istart = np.where(df_rain_new['datetime'] == df_rain['datetime'].iloc[-1])[0][0] + 1
                for irow in range(istart, len(df_rain_new)):
                    df_rain.at[len(df_rain), 'datetime'] = df_rain_new.at[irow, 'datetime']
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
            df_rain[sta] = np.nan
            if sta in df_rain_new.columns:
                for row_new in np.where(~df_rain_new[sta].isna())[0]:
                    row = np.where(df_rain['datetime'] == df_rain_new.at[row_new, 'datetime'])[0]
                    if len(row) == 0:
                        raise ValueError('Datetime mismatch when updating rain data')
                    df_rain.at[row[0], sta] = df_rain_new.at[row_new, sta]
        print(f'\rUpdated rain data for station {ista+1} of {len(still_active)}: {sta}', end='', flush=True)
print('saving rain update')
df_rain.to_csv(opcsv, index=False)