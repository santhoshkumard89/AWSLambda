from yahooquery import Ticker
import datetime
import json
import boto3
import time

# Your goal is to get per-hour stock price data for a time range for the ten stocks specified in the doc. 
# Further, you should call the static info api for the stocks to get their current 52WeekHigh and 52WeekLow values.
# You should craft individual data records with information about the stockid, price, price timestamp, 52WeekHigh and 52WeekLow values and push them individually on the Kinesis stream

kinesis = boto3.client('kinesis', region_name='us-east-1') #Modify this line of code according to your requirement.

Tikers = ['MSFT', 'MVIS', 'GOOG', 'SPOT', 'INO', 'OCGN', 'ABML', 'RLLCF', 'JNJ', 'PSFE']

for symbol in Tikers:

    TData = Ticker(symbol, asynchronous=True)
    df = TData.history(period='5d', interval='1h')
    df.reset_index(inplace=True)
    df.rename(columns = {'symbol':'Ticker', 'date': 'Datetime', 'close':'Close'}, inplace = True)
    df['52WeekLow'] = TData.summary_detail[symbol]['fiftyTwoWeekLow']
    df['52WeekHigh'] = TData.summary_detail[symbol]['fiftyTwoWeekHigh']
    # print(df[['Ticker', 'Datetime', 'Close', '52WeekLow', '52WeekHigh']])
    
    data = df[['Ticker', 'Datetime', 'Close', '52WeekLow', '52WeekHigh']]
    
    for index, row in data.iterrows():
        payload = {
            'Ticker': row['Ticker'],
            'Datetime': str(row['Datetime']),
            'Close': str(round(row['Close'],2)),
            '52WeekLow': str(round(row['52WeekLow'],2)),
            '52WeekHigh': str(round(row['52WeekHigh'],2)),
        }
        print(payload)
        response = kinesis.put_record(
            StreamName="YahooStream",
            Data=json.dumps(payload),
            PartitionKey=row['Ticker']
        )
        print(response)