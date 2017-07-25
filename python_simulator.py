from flask import Flask
import os
import requests
import json
import base64
import websocket
import datetime
import time
import pandas as pd


uaaUrl = "https://yourUAAID.predix-uaa.run.aws-usw02-pr.ice.predix.io/oauth/token"
tsUrl = "https://time-series-store-predix.run.aws-usw02-pr.ice.predix.io/v1/datapoints"

zoneId = "your timeseries zone id"
token = str(base64.b64encode(b'client:secret'))[2:-1]

tstags = "https://time-series-store-predix.run.aws-usw02-pr.ice.predix.io/v1/tags"
ingesturl="wss://gateway-predix-data-services.run.aws-usw02-pr.ice.predix.io/v1/stream/messages"

app = Flask(__name__)
port = int(os.getenv("PORT", 64781))

def doIngest(ingesturl,payload,tsUrl, uaaUrl, token, zoneId,k):
    
    headers = {
        'authorization': "Basic " + token,
        'cache-control': "no-cache",
        'content-type': "application/x-www-form-urlencoded"
    }
    response = requests.request('POST', uaaUrl, data="grant_type=client_credentials", headers=headers)
    token = json.loads(response.text)['access_token']
    headers = {
        'authorization': "Bearer " + token,
        'predix-zone-id': zoneId,
        'content-type': "application/json",
        'Origin': "https://time-series-store-predix.run.aws-usw02-pr.ice.predix.io",
        'cache-control': "no-cache"
    }
    try:
        ws = websocket.create_connection(ingesturl, header=headers)
        ws.send(payload)
        result =  ws.recv()
        ws.close()
        return result
    except  Exception:
        return "failed"
    


@app.route('/')
def simulator():
    
    while True:
        df=pd.read_csv("zeus.csv")
        #j=0
        for index, row in df.iterrows():
            msg = row
            
            current_time = datetime.datetime.now(datetime.timezone.utc)
            unix_timestamp = int(round(current_time.timestamp(),0)) # works if Python >= 3.3

            for k in range ((len(msg)-1)):
                        
                payload_tm="{\n  \"messageId\": \"1453338376231\",\n  \"body\": [\n    {\n      \"name\": \"zeus:"+ str(df.columns[k+1]) +"\",\n      \"datapoints\": [\n        [\n          " +str(unix_timestamp)+"000,\n          "+str(round (msg[k+1],2))+",\n          3\n        ]\n      ],\n      \"attributes\": {\n        \"host\": \"server1\",\n        \"customer\": \"Zeus\"\n      }\n    }\n  ]\n}"
                
                try:
                    rrr=doIngest(ingesturl,payload_tm,tsUrl, uaaUrl, token, zoneId,k)
                except  Exception:
                    rrr="failed"
                    pass

                                
            
            time.sleep(5)
        


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
    
    
