import requests
import json
import pandas as pd

import datetime

url = "https://skew.com/api/tsdb/query?title=eth_futures__aggregated_daily_volumes"

payload = "{\"from\":\"0\",\"to\":\"1599469785431\",\"queries\":[{\"datasourceId\":2,\"format\":\"time_series\",\"rawSql\":\"SELECT exchange, date as time, volume\\nFROM public.all_futures_daily\\nWHERE $__timeFilter(date) and underlying = 'ETH' and exchange IN (SELECT * FROM public.approved_exchanges)\\nORDER BY date asc, exchange asc\",\"refId\":\"A\",\"intervalMs\":1200000,\"maxDataPoints\":131}]}"
headers = {
  'Content-Type': 'application/json',
  'Cookie': 'AWSALB=0aQheetsziHkkbf3efzU01sPITl13pJ0UvVTD5fMNdPuxIJ9yut22hgUDObCGSatffM0+mx5HP9gttDDmKQKtNCIOvR3XKEfVWGgiEPvWuKHfOU9BsYXC9jCx9Mv; AWSALBCORS=0aQheetsziHkkbf3efzU01sPITl13pJ0UvVTD5fMNdPuxIJ9yut22hgUDObCGSatffM0+mx5HP9gttDDmKQKtNCIOvR3XKEfVWGgiEPvWuKHfOU9BsYXC9jCx9Mv'
}

response = requests.request("POST", url, headers=headers, data = payload)

data = json.loads(response.text.encode('utf8'))

series = data['results']['A']['series']
headers = ['timestamp']
time_point = []
for serial in series:
    headers.append(serial['name'])
    for point in serial['points']:
        if point[1] not in time_point:
            time_point.append(point[1])

    # print(serial['name'])
    # print(serial['points'])
print(headers)

time_point.sort()
# print(time_point)
def getVal(arr,target):
    # print(arr)
    result = list(filter(lambda ele: ele[1] == target, arr))
    if len(result)>0:
        return result[0][0]
    else:
        return None
data=[]
# data.append(headers)
for time in time_point:

    temp = [""+datetime.datetime.utcfromtimestamp(time/1000.0).strftime('%Y-%m-%d %H:%M:%S')+""]
    for serial in series:
        temp.append(getVal(serial['points'], time))
    data.append(temp)
df = pd.DataFrame(data)
#
# csv_data = df.to_csv(index=False)
#
df.to_csv('./eth_futures__aggregated_daily_volumes.csv',header=headers, index=False)
