import requests
import json
import pandas as pd

import datetime

url = "https://skew.com/api/tsdb/query?title=eth_options_volumes"

payload = "{\"from\":\"0\",\"to\":\"1599475460795\",\"queries\":[{\"datasourceId\":2,\"format\":\"time_series\",\"rawSql\":\"SELECT DATE_TRUNC('day', date) as time, sum(size*index) as \\\"Deribit\\\"\\nFROM public.deribit_trade\\nWHERE (underlying = 'ETH') and $__timeFilter(date)\\nGROUP BY time\\nORDER BY time asc\",\"refId\":\"A\",\"intervalMs\":1200000,\"maxDataPoints\":131},{\"datasourceId\":2,\"format\":\"time_series\",\"rawSql\":\"SELECT DATE_TRUNC('day', date) as time, sum(size*index) as \\\"OKEx\\\"\\nFROM public.okex_opt_trades\\nWHERE (underlying = 'ETH') and $__timeFilter(date)\\nGROUP BY time\\nORDER BY time asc\",\"refId\":\"B\",\"intervalMs\":1200000,\"maxDataPoints\":131}]}"
headers = {
  'Content-Type': 'application/json',
  'Cookie': 'AWSALB=RtDZ0jWDc1Xv+UsRJ2oID0ivyh1YJDRcafQ/jEoVaXYAdlUZ3nraP4u6Lr6YusfJZQF7vzkY8l/w6UAFMRSJS9YqdQU3meFb19Wr/L/itSflYlVUUGktXJoSu02K; AWSALBCORS=RtDZ0jWDc1Xv+UsRJ2oID0ivyh1YJDRcafQ/jEoVaXYAdlUZ3nraP4u6Lr6YusfJZQF7vzkY8l/w6UAFMRSJS9YqdQU3meFb19Wr/L/itSflYlVUUGktXJoSu02K'
}
abc = ["A","B"]

response = requests.request("POST", url, headers=headers, data = payload)

data = json.loads(response.text.encode('utf8'))
series = data['results']['A']['series']

for a in abc:
    if a != 'A':
        series.append(data['results'][a]['series'][0])
# print(series)


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
df.to_csv('./eth_options_volumes.csv',header=headers, index=False)
