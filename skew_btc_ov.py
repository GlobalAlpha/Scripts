import requests
import json
import pandas as pd

import datetime

url = "https://skew.com/api/tsdb/query?title=btc_options_volumes"

payload = "{\"from\":\"0\",\"to\":\"1599474483893\",\"queries\":[{\"datasourceId\":2,\"format\":\"time_series\",\"rawSql\":\"SELECT DATE_TRUNC('day', date) as time, sum(size*index) as \\\"Deribit\\\"\\nFROM public.deribit_trade\\nWHERE (underlying != 'ETH' and underlying IS NOT NULL) and $__timeFilter(date)\\nGROUP BY time\\nORDER BY time asc\",\"refId\":\"A\",\"intervalMs\":1200000,\"maxDataPoints\":131},{\"datasourceId\":2,\"format\":\"time_series\",\"rawSql\":\"SELECT volume_table.time as time, volume_table.volume*index_table.price as \\\"LedgerX\\\"\\nFROM\\n  (SELECT\\n\\ttrade_date as time, sum(volume * (CASE WHEN LEFT(instrument_name, 1) != 'c' THEN 1 ELSE 0.01 END)) as volume\\nFROM\\n\\tpublic.ledgerx_trades\\nGROUP BY\\n\\ttrade_date) volume_table\\n  LEFT OUTER JOIN\\n  (SELECT\\n    date_trunc('day', date) as time,\\n    avg(index) as price\\n  FROM\\n    public.deribit_trade\\n  WHERE\\n    $__timeFilter(date) AND (underlying = 'BTC')\\n  GROUP BY\\n    time\\n  ORDER BY\\n    time asc) index_table\\nON volume_table.time = index_table.time\\nORDER BY\\n  volume_table.time\",\"refId\":\"B\",\"intervalMs\":1200000,\"maxDataPoints\":131},{\"datasourceId\":2,\"format\":\"time_series\",\"rawSql\":\"SELECT volume_table.time as time, volume_table.volume*index_table.price as \\\"Bakkt\\\"\\nFROM\\n  (SELECT\\n    date as time,\\n    sum(COALESCE(volume, 0)+COALESCE(efpvolume, 0)+COALESCE(efsvolume, 0)+COALESCE(blockvolume, 0)) as volume\\n  FROM\\n    public.bakkt_options\\n  WHERE\\n    $__timeFilter(date)\\n  GROUP BY\\n    time) volume_table\\n  LEFT OUTER JOIN\\n  (SELECT\\n    date_trunc('day', date) as time,\\n    avg(index) as price\\n  FROM\\n    public.deribit_trade\\n  WHERE\\n    $__timeFilter(date) AND (underlying IS NULL OR underlying != 'ETH')\\n  GROUP BY\\n    time\\n  ORDER BY\\n    time asc) index_table\\nON volume_table.time = index_table.time\\nORDER BY\\n  volume_table.time\\n\",\"refId\":\"C\",\"intervalMs\":1200000,\"maxDataPoints\":131},{\"datasourceId\":2,\"format\":\"time_series\",\"rawSql\":\"SELECT DATE_TRUNC('day', date) as time, sum(size*index) as \\\"OKEx\\\"\\nFROM public.okex_opt_trades\\nWHERE (underlying = 'BTC') and $__timeFilter(date)\\nGROUP BY time\\nORDER BY time asc\",\"refId\":\"D\",\"intervalMs\":1200000,\"maxDataPoints\":131},{\"datasourceId\":2,\"format\":\"time_series\",\"rawSql\":\"SELECT\\n  volume_table.time,\\n  volume_table.volume*index_table.price*5 as \\\"CME\\\"\\nFROM\\n  (SELECT\\n   TO_DATE(\\\"Trade Date\\\" :: VARCHAR(8), 'YYYYMMDD') as time,\\n    sum(\\\"Total Volume\\\") as volume\\n  FROM\\n    public.cme_eod_options\\n  GROUP BY\\n    time) volume_table\\nLEFT OUTER JOIN\\n  (SELECT\\n    date_trunc('day', date) as time,\\n    avg(index) as price\\n  FROM\\n    public.deribit_trade\\n  WHERE\\n   $__timeFilter(date) AND (underlying IS NULL OR underlying != 'ETH')\\n  GROUP BY\\n    time) index_table\\nON volume_table.time = index_table.time\",\"refId\":\"E\",\"intervalMs\":1200000,\"maxDataPoints\":131},{\"datasourceId\":2,\"format\":\"time_series\",\"rawSql\":\"SELECT DATE_TRUNC('day', date) as time, sum(size*index) as \\\"bit.com\\\"\\nFROM public.bitcom_opt_trades\\nWHERE (underlying = 'BTC') and $__timeFilter(date)\\nGROUP BY time\\nORDER BY time asc\",\"refId\":\"F\",\"intervalMs\":1200000,\"maxDataPoints\":131},{\"datasourceId\":2,\"format\":\"time_series\",\"rawSql\":\"SELECT DATE_TRUNC('day', date) as time, sum(size*index) as \\\"Huobi\\\"\\nFROM public.huobi_opt_trades\\nWHERE (underlying = 'BTC') and $__timeFilter(date)\\nGROUP BY time\\nORDER BY time asc\",\"refId\":\"G\",\"intervalMs\":1200000,\"maxDataPoints\":131}]}"
headers = {
  'Content-Type': 'application/json',
  'Cookie': 'AWSALB=8Je4q0g9PHsUGtqq8a9kPPeYP41OeZnllVODwhQwebHyKhrcRC11yYzFSc52UcwPE/akQNPlBPMNo/kRQceqYMfsW/wPJVKXsJMrkg8oYI836R8FvHUEgdrqJBaa; AWSALBCORS=8Je4q0g9PHsUGtqq8a9kPPeYP41OeZnllVODwhQwebHyKhrcRC11yYzFSc52UcwPE/akQNPlBPMNo/kRQceqYMfsW/wPJVKXsJMrkg8oYI836R8FvHUEgdrqJBaa'
}


abc = ["A","B","C","D","E","F","G"]

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
df.to_csv('./btc_options_volumes.csv',header=headers, index=False)
