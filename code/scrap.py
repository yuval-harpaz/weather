import os
os.chdir(os.environ['HOME']+'/weather')
import pandas as pd
import sys
from datetime import datetime
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

url = f'https://api.ims.gov.il/v1/envista/stations/{stationid}/data/{channels[0]}?from={from_date.replace("-","/")}&to={to_date.replace("-","/")}'
response = requests.request("GET", url, headers=headers)
txt = response.text.encode('utf8')
data = json.loads(txt)
data = data['data']