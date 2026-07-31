import os
os.chdir(os.environ['HOME']+'/weather')
import sys
sys.path.append(os.environ['HOME']+'/weather/code')
from weather import *
import numpy as np
ims_api_token = os.environ.get('IMS_API_TOKEN')
headers = {'Authorization': 'ApiToken '+ims_api_token}
got_grad = []
got_all = []
got_nip = []
got_diff = []
for ii in range(len(df_sta)):
    stationid = df_sta.at[ii, 'stationId']
    name = df_sta.at[ii, 'name']
    monitors = df_sta.at[ii, 'monitors']
    if 'Grad' in monitors:
        got_grad.append(name)
        if 'WS' in monitors and 'RH' in monitors and 'TD' in monitors:
            got_all.append(name)
    if 'NIP' in monitors:
        got_nip.append(name)
    if 'Diff' in monitors:
        got_diff.append(name)

ii = np.where(df_sta['name'].values == 'TEL YOSEF')[0][0]
stationid = df_sta.at[ii, 'stationId']
name = df_sta.at[ii, 'name']
monitors = df_sta.at[ii, 'monitors']
mnt = eval(monitors)
channels = [m['channelId'] for m in mnt if m['name'] in ['Grad', 'TD', 'RH', 'WS']]
from_date = '2025-10-07'
to_date = '2025-10-10'
url_all = f'https://api.ims.gov.il/v1/envista/stations/{stationid}/data?from={from_date.replace("-","/")}&to={to_date.replace("-","/")}'
time_all = []
time_chan = []
for ii in range(20):
    time0 = time.time()
    response = requests.request("GET", url_all, headers=headers)
    txt = response.text.encode('utf8')
    data = json.loads(txt)
    data = data['data']
    time1 = time.time()
    time_all.append(time1 - time0)
    print(f"all: {time1 - time0}")
    time0 = time.time()
    for chan in channels:
        time.sleep(0.1)
        url_chan = url_all.replace('data?', 'data/'+str(chan)+'?')
        response = requests.request("GET", url_chan, headers=headers)
        txt = response.text.encode('utf8')
        data = json.loads(txt)
        data = data['data']
    time1 = time.time()
    time_chan.append(time1 - time0)
    print(f"chan: {time1 - time0}")

