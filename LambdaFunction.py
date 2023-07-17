import json
import boto3
from datetime import datetime
from pprint import pprint
import base64

def lambda_handler(event, context):
    
    dynamodb_res = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb_res.Table('YahooPOI')
    
    sns_client = boto3.client("sns")
    
    for records in event['Records']:
        record = records['kinesis']['data']
        decdata = base64.b64decode(record)
        decdata = str(decdata, 'utf-8')
        data = json.loads(decdata)
        pprint(data)
    
        payload = {
            'Ticker': data['Ticker'],
            'Datetime': data['Datetime'][:10],
            'Close': data['Close'],
            '52WeekLow': data['52WeekLow'],
            '52WeekHigh': data['52WeekHigh'],
        }
    
        WL52POI = round(float(data['52WeekLow']) * 1.2,2)
        WH52POI = round(float(data['52WeekHigh']) * 0.8,2)
        Close = float(data['Close'])
        print(WH52POI, Close, WL52POI)
    
        if((WH52POI <= Close) or (Close <= WL52POI)):
            response = table.get_item(Key={
                                        'Ticker': payload['Ticker'],
                                        'Datetime': payload['Datetime']
                                    })
            
            items = response.get("Items", None)
            
            if(items):
                print(response)
            else:
                table.put_item(Item= payload)
                sns_client.publish(
                    TopicArn="arn:aws:sns:us-east-1:195039156226:YahooSNS",
                    Message=json.dumps(payload)
                )