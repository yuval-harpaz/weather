import os
import pandas as pd
import json
import requests
import sys
import numpy as np
import time
from glob import glob
from datetime import datetime, timedelta
# https://data.gov.il/dataset/481
# https://ims.gov.il/sites/default/files/2023-01/API%20explanation_1.pdf
# https://ims.gov.il/he/ObservationDataAPI

f = open('token.txt', 'r')
token = f.read()
f.close()
token = token.replace('\n', '')
headers = {'Authorization': 'ApiToken '+token}

def timing():
    """
    Measures and compares API response times for different query methods.
    
    Inputs:
        None (uses hardcoded test dates and station 121)
    
    Outputs:
        None (prints median response times for different query types to console)
    """
    from_date='2025-10-07'
    to_date='2025-10-10'
    url = [f'https://api.ims.gov.il/v1/envista/stations/121/data/1?from={from_date.replace("-","/")}&to={to_date.replace("-","/")}']
    url.append(f'https://api.ims.gov.il/v1/envista/stations/121/data?from={from_date.replace("-","/")}&to={to_date.replace("-","/")}')
    url.append(f'https://api.ims.gov.il/v1/envista/stations/121/data?from=2025/10/09&to={to_date.replace("-","/")}')
    times = np.zeros((5, len(url)))
    times[:] = np.nan
    for itry in range(5):
        for iurl in range(len(url)):
            t0 = time.time()
            response = requests.request("GET", url[iurl], headers=headers)
            t1 = time.time()
            # print(f'url {iurl} try {itry} time {t1 - t0:.2f} sec')
            time.sleep(0.2)
            txt = response.text.encode('utf8')
            if len(txt) == 0 or 'error.png' in str(txt):
                pass
            else:
                times[itry, iurl] = t1 - t0
        print(times[itry])
    print(f'median times for one channel: {np.nanmedian(times[:,0]):.2f} sec')
    print(f'median times for all channels: {np.nanmedian(times[:,1]):.2f} sec')
    print(f'median times for all channels one day: {np.nanmedian(times[:,2]):.2f} sec')

def update_stations():
    """
    Updates the stations CSV file with new stations from the IMS API.
    
    Inputs:
        None (reads from data/ims_stations.csv, fetches from IMS API)
    
    Outputs:
        None (updates data/ims_stations.csv if new stations are found)
    """
    url = 'https://api.ims.gov.il/v1/Envista/stations'
    response = requests.request("GET", url, headers=headers)
    data = json.loads(response.text.encode('utf8'))
    df_sta = pd.DataFrame(data)
    prev = pd.read_csv('data/ims_stations.csv')
    # look for new stations
    df_new = df_sta[~df_sta['stationId'].isin(prev['stationId'].values)]
    df_discontinued = prev[~prev['stationId'].isin(df_sta['stationId'].values)]
    if len(df_discontinued) > 0:
        print('discontinued stations:')
        print(df_discontinued[['stationId', 'name']])
        raise Exception('discontinued stations found, please check!')
    # makew sure station names are as before
    for ista in range(len(prev)):
        stationid = prev['stationId'].values[ista]
        name_prev = prev['name'].values[ista]
        name_new = df_sta['name'].values[df_sta['stationId'] == stationid][0]
        if name_prev != name_new:
            print(f'station name changed for id {stationid} from {name_prev} to {name_new}')
            raise Exception('station name changed, please check!')
    if len(df_new) > 0:
        df_sta.to_csv('data/ims_stations.csv', index=False)

df_sta = pd.read_csv('data/ims_stations.csv')

def update_regions():
    """
    Updates the regions CSV file with new regions from the IMS API.
    
    Inputs:
        None (reads from data/ims_regions.csv, fetches from IMS API)
    
    Outputs:
        None (updates data/ims_regions.csv if new regions are found)
    """
    url = 'https://api.ims.gov.il/v1/Envista/regions'
    response = requests.request("GET", url, headers=headers)
    data = json.loads(response.text.encode('utf8'))
    df_reg = pd.DataFrame(data)
    prev = pd.read_csv('data/ims_regions.csv')
    df_reg_new = df_reg[~df_reg['regionId'].isin(prev['regionId'].values)]
    if len(df_reg_new) > 0:
        df_reg.to_csv('data/ims_regions.csv', index=False)

def update_activity(new=False):
    """
    Updates station activity data with earliest and latest observation times.
    
    Inputs:
        new (bool): If True, creates new activity DataFrame; if False, updates existing
                   data/ims_activity.csv. Default is False.
    
    Outputs:
        None (saves updated data to data/ims_activity.csv)
    """
    if new:
        df_activity = pd.DataFrame(columns=['stationId', 'name', 'earliest', 'latest'])
        df_activity['stationId'] = df_sta['stationId']
        df_activity['name'] = df_sta['name']
    else:
        df_activity = pd.read_csv('data/ims_activity.csv')
    for ista in range(len(df_sta)):
        stationid = df_sta['stationId'].values[ista]
        # check earliest if not empty cell
        if type(df_activity.at[ista, 'earliest']) != str or len(df_activity.at[ista, 'earliest']) == 0:
            url = f'https://api.ims.gov.il/v1/envista/stations/{stationid}/data/1/earliest'
            for itry in range(10):
                response = requests.request("GET", url, headers=headers)
                txt = response.text.encode('utf8')
                try:
                    data = json.loads(txt)
                    df_activity.at[ista, 'earliest'] = data['data'][0]['datetime']
                    break
                except (json.JSONDecodeError, KeyError, IndexError):
                    # df_activity.at[ista, 'earliest'] = ''
                    time.sleep(0.3)
        url = f'https://api.ims.gov.il/v1/envista/stations/{stationid}/data/1/latest'
        #check latest for 2025 active stations
        if str(df_activity.at[ista, 'latest']) > '2025-01-01T00:00:00':
            fail = False
            for itry in range(40):
                response = requests.request("GET", url, headers=headers)
                txt = response.text.encode('utf8')
                try:
                    data = json.loads(txt)
                    df_activity.at[ista, 'latest'] = data['data'][0]['datetime']
                    fail = False
                    break
                except (json.JSONDecodeError, KeyError, IndexError):
                    # df_activity.at[ista, 'latest'] = ''
                    fail = True
                    time.sleep(0.1)
            if fail:
                print(f'failed to get latest for station {stationid}')
        msg = f'checking activity for station {ista+1}/{len(df_sta)}'
        print(f'\r{msg:<80}', end='', flush=True)
    print()  # Final newline after loop completes
    df_activity.to_csv('data/ims_activity.csv', index=False)
   
def query_rain(station='HAFEZ HAYYIM', from_date='2025-10-07', to_date='2025-10-10', monitor='Rain'):
    """
    Queries rain data from IMS API for a specific station and date range.
    
    Inputs:
        station (str): Station name (e.g., 'HAFEZ HAYYIM'). Default is 'HAFEZ HAYYIM'.
        from_date (str): Start date in 'YYYY-MM-DD' format. Default is '2025-10-07'.
        to_date (str): End date in 'YYYY-MM-DD' format, or None for daily query. Default is '2025-10-10'.
        monitor (str): Monitor type ('Rain' or 'Rain_1_min'). Default is 'Rain'.
    
    Outputs:
        list or None: List of dictionaries containing rain data from API, or None if station 
                     doesn't have the specified monitor or query fails.
    """
    stationid = df_sta['stationId'].values[df_sta['name'] == station][0]
    monitors = df_sta['monitors'].values[df_sta['name'] == station][0]
    if not f"'{monitor}'" in monitors:
        print(f'station {station} has no {monitor} monitor')
        return None
    irain = monitors.index(f"'{monitor}'")
    tmp = monitors[:irain]  # find which channel has name 'Rain'
    channel = int(tmp[::-1][tmp[::-1].index(","):tmp[::-1].index(":'dI")][1:].strip()[::-1]) # find last '
    if to_date is None:  # daily
        date = from_date
        url = f'https://api.ims.gov.il/v1/envista/stations/{stationid}/data/{channel}/daily/{date[:4]}/{date[5:7]}/{date[8:10]}'
    else:
        url = f'https://api.ims.gov.il/v1/envista/stations/{stationid}/data/{channel}?from={from_date.replace("-","/")}&to={to_date.replace("-","/")}'
    data = None
    for itry in range(10):
        response = requests.request("GET", url, headers=headers)
        txt = response.text.encode('utf8')
        if len(txt) == 0 or 'error.png' in str(txt):
            time.sleep(0.1)
        else:                
            data = json.loads(txt)
            data = data['data']
            break
    return data

def hour_vector(from_date, to_date):
    """
    Generates a list of hourly timestamps between two dates.
    
    Inputs:
        from_date (str): Start date in 'YYYY-MM-DD' format
        to_date (str): End date in 'YYYY-MM-DD' format
    
    Outputs:
        list: List of datetime strings in 'YYYY-MM-DD HH:MM' format for each hour 
             between from_date and to_date (inclusive)
    """
    date_format = "%Y-%m-%d"
    start_date = datetime.strptime(from_date, date_format)
    end_date = datetime.strptime(to_date, date_format)
    delta = end_date - start_date
    hours = []
    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        for h in range(24):
            hours.append((day + timedelta(hours=h)).strftime("%Y-%m-%d %H:%M"))
    return hours

def rain_1h(stations=None, from_date='2025-10-07', to_date='2025-10-10', save_csv=True):
    """
    Collects and aggregates hourly rain data for specified stations and date range.
    
    Inputs:
        stations (list or None): List of station names to query. If None, queries all stations.
                                Default is None.
        from_date (str): Start date in 'YYYY-MM-DD' format. Default is '2025-10-07'.
        to_date (str): End date in 'YYYY-MM-DD' format. Default is '2025-10-10'.
        save_csv (bool or str): If True, saves to default CSV path; if string, uses as custom path;
                               if False, doesn't save. Default is True.
    
    Outputs:
        pandas.DataFrame: DataFrame with 'datetime' column and one column per station containing 
                         hourly rain amounts in mm. Saves to CSV file in data/ directory.
    """
    
    if from_date[5:] == '01-01' and to_date[5:] == '12-31':
        yearly = True
        year = from_date[:4]
        opcsv = f'data/rain_{year}.csv'
    else:
        yearly = False
        year = None
        if type(save_csv) == bool:
            if save_csv:
                opcsv = f'data/rain_{from_date}_to_{to_date}.csv'
            else:
                opcsv = ''
    df_activity = pd.read_csv('data/ims_activity.csv')
    hours = hour_vector(from_date, to_date)
    # if hours extend beyond now, limit to now
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    if hours[-1] > now_str:
        hours = [h for h in hours if h <= now_str]
    if stations is None:
        stations = df_sta['name'].values
    # for station in stations:
    #     df_rain[station] = 0.0
    if os.path.exists(opcsv):
        df_rain = pd.read_csv(opcsv)
    else:
        df_rain = pd.DataFrame(columns=['datetime'])
        df_rain['datetime'] = hours
        if save_csv:
            df_rain.to_csv(opcsv, index=False)
    count = 0
    for station in stations:
        count += 1
        if yearly and year < '2017' and '_1m' in station:
            # msg = f'skipping {station} before 2017'
            # print(f'\r{msg:<80}', end='', flush=True)
            continue
        t1 = time.time()
        if save_csv:
            df_rain = pd.read_csv(opcsv)
        
        if station in df_rain.columns:
            msg = f'{station} already in rain data'
            print(f'\r{msg:<80}', end='', flush=True)
            continue
        # check if station has Rain monitor
        monitors = df_sta['monitors'].values[df_sta['name'] == station][0]
        if "'Rain'" not in monitors and "'Rain_1_min'" not in monitors:
            msg = f'skipping {station} no Rain monitor'
            print(f'\r{msg:<80}', end='', flush=True)
            continue

        earliest = df_activity['earliest'].values[df_activity['name'] == station][0]
        latest = df_activity['latest'].values[df_activity['name'] == station][0]
        if from_date < '2026':  # don't skip stations active in 2026
            if from_date > latest or to_date < earliest:
                msg = f'skipping inactive {station}'
                print(f'\r{msg:<80}', end='', flush=True)
                continue
        if '_1m' in station:
            monitor = 'Rain_1_min'
        else:
            monitor = 'Rain'
        data = query_rain(station=station, from_date=from_date, to_date=to_date, monitor=monitor)
        if data is None:
            msg = f'None data for {station}!'
            print(f'\r{msg:<80}')
            continue
        # if monitor == 'Rain_1_min':
        data = [d for d in data if d['channels'][0]['value'] > 0]
        if len(data) == 0:
            continue
        data = [d for d in data if d['channels'][0]['valid'] == True and d['channels'][0]['status'] == 1]
        if len(data) == 0:
            continue
        df_rain[station] = np.nan
        for idata in range(len(data)):
            date_time = data[idata]['datetime'][:16].replace('T', ' ')
            date_time = date_time[:-3]+':00'  # round to hour
            row = np.where(df_rain['datetime'].values == date_time)[0][0]
            if data[idata]['channels'][0]['valid'] == True and data[idata]['channels'][0]['status'] == 1:
                value = np.round(data[idata]['channels'][0]['value'], 1)
                df_rain.at[row, station] = np.nansum([df_rain.at[row, station], value])
        t2 = time.time()
        if yearly:
            msg = f'updated {from_date[:4]} rain for {station} {t2 - t1:.2f}s ({count}/{len(stations)})'
        else:
            msg = f'updated rain for {station} {t2 - t1:.2f}s ({count}/{len(stations)})'
        print(f'\r{msg:<80}', end='', flush=True)
        if save_csv:
            df_rain.to_csv(opcsv, index=False)
    print()  # Final newline after loop completes
    return df_rain

def shrink_rain_data(year='2024'):
    """
    Removes rows with all NaN values from yearly rain data to reduce file size.
    Inputs:
        year (str): Year to process (e.g., '2024'). Default is '2024'.
    Outputs:
        pandas.DataFrame: DataFrame with only rows containing at least one non-NaN rain value
    """
    df_rain = pd.read_csv(f'data/rain_{year}.csv')
    rows_to_keep = np.isnan(df_rain.iloc[:, 1:]).all(axis=1) == False
    df_rain = df_rain[rows_to_keep]
    return df_rain


if __name__ == '__main__':
    # update_stations()
    # update_regions()
    # # update_activity()
    # example query
    if len(sys.argv) == 1:
        print('Query rain data between dates. examples:')
        print('python weather.py 2023-10-01 2023-10-10')
        print('python weather.py 2023')
    else:
        if len(sys.argv) == 2:
            year = sys.argv[1]
            from_date = f'{year}-01-01'
            to_date = f'{year}-12-31'
        elif len(sys.argv) == 3:
            from_date = sys.argv[1]
            to_date = sys.argv[2]
        else:
            raise Exception('invalid arguments')
        df_all = rain_1h(from_date=from_date, to_date=to_date)

        # print(np.sum(df_all.values[:,1:], axis=0))

    # data = query_rain(station='HAFEZ HAYYIM', from_date='2023-10-01', to_date='2023-10-10', monitor='Rain')
    # data1m = query_rain(station='HAFEZ HAYYIM_1m', from_date='2023-10-01', to_date='2023-10-10', monitor='Rain_1_min')
    # df_HH = rain_1h(stations=['HAFEZ HAYYIM', 'HAFEZ HAYYIM_1m'], from_date='2023-10-01', to_date='2023-10-10')
    # df_all = rain_1h(from_date='2023-10-01', to_date='2023-10-10')
    # df_all.to_csv('~/Documents/ims.csv', index=False)
    # print(np.sum(df_all.values[:,1:], axis=0))
