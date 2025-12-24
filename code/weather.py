import pandas as pd
import json
import requests
import os
import numpy as np
import time
from glob import glob
# https://data.gov.il/dataset/481
# https://ims.gov.il/sites/default/files/2023-01/API%20explanation_1.pdf
# https://ims.gov.il/he/ObservationDataAPI

f = open('token.txt', 'r')
token = f.read()
f.close()
token = token.replace('\n', '')
headers = {'Authorization': 'ApiToken '+token}
def update_stations():
    url = 'https://api.ims.gov.il/v1/Envista/stations'
    response = requests.request("GET", url, headers=headers)
    data = json.loads(response.text.encode('utf8'))
    df_sta = pd.DataFrame(data)
    prev = pd.read_csv('data/ims_stations.csv')
    # look for new stations
    df_new = df_sta[~df_sta['stationId'].isin(prev['stationId'].values)]
    if len(df_new) > 0:
        df_sta.to_csv('data/ims_stations.csv', index=False)

df_sta = pd.read_csv('data/ims_stations.csv')

def update_regions():
    url = 'https://api.ims.gov.il/v1/Envista/regions'
    response = requests.request("GET", url, headers=headers)
    data = json.loads(response.text.encode('utf8'))
    df_reg = pd.DataFrame(data)
    prev = pd.read_csv('data/ims_regions.csv')
    df_reg_new = df_reg[~df_reg['regionId'].isin(prev['regionId'].values)]
    if len(df_reg_new) > 0:
        df_reg.to_csv('data/ims_regions.csv', index=False)

def update_activity():
    df_activity = pd.DataFrame(columns=['stationId', 'name', 'earliest', 'latest'])
    df_activity['stationId'] = df_sta['stationId']
    df_activity['name'] = df_sta['name']
    for ista in range(len(df_sta)):
        stationid = df_sta['stationId'].values[ista]
        url = f'https://api.ims.gov.il/v1/envista/stations/{stationid}/data/1/earliest'
        response = requests.request("GET", url, headers=headers)
        txt = response.text.encode('utf8')
        try:
            data = json.loads(txt)
            df_activity.at[ista, 'earliest'] = data['data'][0]['datetime']
        except (json.JSONDecodeError, KeyError, IndexError):
            df_activity.at[ista, 'earliest'] = ''
        url = f'https://api.ims.gov.il/v1/envista/stations/{stationid}/data/1/latest'
        response = requests.request("GET", url, headers=headers)
        txt = response.text.encode('utf8')
        try:
            data = json.loads(txt)
            df_activity.at[ista, 'latest'] = data['data'][0]['datetime']
        except (json.JSONDecodeError, KeyError, IndexError):
            df_activity.at[ista, 'latest'] = ''
    df_activity.to_csv('data/ims_activity.csv', index=False)
    
def query_rain(station='HAFEZ HAYYIM', from_date='2025-10-07', to_date='2025-10-10', monitor='Rain'):
    stationid = df_sta['stationId'].values[df_sta['name'] == station][0]
    monitors = df_sta['monitors'].values[df_sta['name'] == station][0]
    if not f"'{monitor}'" in monitors:
        print(f'station has no {monitor} monitor')
        return None
    irain = monitors.index(f"'{monitor}'")
    tmp = monitors[:irain]  # find which channel has name 'Rain'
    channel = int(tmp[::-1][tmp[::-1].index(","):tmp[::-1].index(":'dI")][1:].strip()[::-1]) # find last '
    if to_date is None:  # daily
        date = from_date
        url = f'https://api.ims.gov.il/v1/envista/stations/{stationid}/data/{channel}/daily/{date[:4]}/{date[5:7]}/{date[8:10]}'
    else:
        url = f'https://api.ims.gov.il/v1/envista/stations/{stationid}/data/{channel}?from={from_date.replace("-","/")}&to={to_date.replace("-","/")}'
    response = requests.request("GET", url, headers=headers)
    txt = response.text.encode('utf8')
    #save to json
    # with open(f'data/{station}_{date}_{monitor}.json', 'w') as f:
    #     f.write(txt.decode('utf8'))
    data = json.loads(txt)
    data = data['data']
    # sum values for valid
    sum_rain = [0]
    date = [data[0]['datetime'][:10]]
    for idata in range(len(data)):
        current_date = data[idata]['datetime'][:10]
        if current_date != date[-1]:
            sum_rain.append(0)
            date.append(current_date)
        if data[idata]['channels'][0]['valid'] == True and data[idata]['channels'][0]['status'] == 1:
            value = data[idata]['channels'][0]['value']
            print(data[idata]['datetime'], value)
            sum_rain[-1] += value
        else:
            print('buga')
    return data

names = sorted([x for x in df['name'].values if '_1m' not in x])
names = [x for x in names if not x.split(' ')[-1].isnumeric()]
# df.to_csv('data/ims_stations.csv', index=False)
prefs = ['tempmax', 'tmpmin', 'rain']
##
for year in [2024]:
    for monthf in range(1, 13):
        month = str(monthf).zfill(2)
        opnames = [f'data/{x}_{year}-{month}.csv' for x in prefs]
        if os.path.isfile(opnames[0]) and os.path.isfile(opnames[1]) and os.path.isfile(opnames[2]):
            print(f'{month}/{year}: files exist')
        else:
            print(f'{month}/{year}')
            dfmin = pd.DataFrame(columns=['date'] + names)
            dfmax = pd.DataFrame(columns=['date'] + names)
            dfrain = pd.DataFrame(columns=['date'] + names)
            t0 = time.time()
            for ista in range(len(names)):
                name = names[ista]
                # print(' query '+name)
                stationid = df['stationId'].values[df['name'] == name][0]
                url = f'https://api.ims.gov.il/v1/envista/stations/' \
                      f'{stationid}/data/monthly/{year}/{month}'
                response = requests.request("GET", url, headers=headers)
                txt = response.text.encode('utf8')
                if len(txt) < 100 or 'error.png' in str(txt):
                    # print('issues reading ' + name)
                    pass
                else:
                    data = json.loads(txt)
                    dates = np.array([x['datetime'][:10] for x in data['data']])
                    dateu = np.unique(dates)
                    dateu = [x for x in dateu if x[:7] == str(year)+'-'+month]  # got next month
                    if len(dfmin) == 0:
                        dfmin['date'] = dateu
                    if len(dfmax) == 0:
                        dfmax['date'] = dateu
                    if len(dfrain) == 0:
                        dfrain['date'] = dateu
                    for idate in range(len(dateu)):
                        date = dateu[idate]
                        idx = np.where(dates == date)[0]
                        rain = np.zeros(len(idx))
                        min_temp = np.zeros(len(idx))
                        max_temp = np.zeros(len(idx))
                        for itime in range(len(idx)):
                            dftime = pd.DataFrame(data['data'][idx[itime]]['channels'])
                            dftime = dftime[dftime['valid'] == True]
                            dftime.reset_index(inplace=True, drop=True)
                            row = np.where(dftime['name'] == 'Rain')[0]
                            if len(row) == 1:
                                rain[itime] = dftime['value'][row[0]]
                            else:
                                rain[itime] = np.nan
                            row = np.where(dftime['name'] == 'TDmax')[0]
                            if len(row) == 1:
                                max_temp[itime] = dftime['value'][row[0]]
                            else:
                                max_temp[itime] = np.nan
                            row = np.where(dftime['name'] == 'TDmin')[0]
                            if len(row) == 1:
                                min_temp[itime] = dftime['value'][row[0]]
                            else:
                                min_temp[itime] = np.nan
                        dfmin.at[idate, name] = np.nanmin(min_temp)
                        dfmax.at[idate, name] = np.nanmax(max_temp)
                        dfrain.at[idate, name] = np.nansum(rain)
            df3 = [dfmax, dfmin, dfrain]
            for idf in range(3):
                df3[idf].to_csv(opnames[idf], index=False)
##
for pref in prefs:
    dfnames = glob(f'data/{pref}_*-*.csv')
    dfs = []
    for ii in range(len(dfnames)):
        df0 = pd.read_csv(dfnames[ii])
        if 'Unnamed: 0' in df0.columns:
            df0.drop('Unnamed: 0', axis=1, inplace=True)
            if any(df0['date'].isnull()):
                raise Exception('nans in date')
        dfs.append(df0)
    dfmerge = pd.concat(dfs, ignore_index=True)
    dfmerge.sort_values('date', ignore_index=True)
    dfmerge.to_csv(f'data/{pref}.csv', index=False)
