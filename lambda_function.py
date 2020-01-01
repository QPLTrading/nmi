import pandas as pd
import numpy as np
import boto3
import json

def lambda_handler(event, context):
    file = event['file']
    folder = 'nmi/'
    bucket_name = 'asb.cloud'
    s3_client = boto3.client('s3',region_name="us-east-2", 
        endpoint_url='https://s3.us-east-2.amazonaws.com')
    s3_resource = boto3.resource('s3', region_name="us-east-2", 
        endpoint_url='https://s3.us-east-2.amazonaws.com')
    try:
        s3_resource.meta.client.download_file(bucket_name, folder+file, '/tmp/'+file)
    except:
        message = {'message':'File cannot be found, please check file name'}
        return {
                 'statusCode':200,
                 'headers':{'Content-Type':'application/json'},
                 'body':json.dumps(message)     
            }
    df = pd.read_csv('/tmp/'+file, parse_dates=True)
    df['AESTTime'] = pd.to_datetime(df['AESTTime'])
    df = df.fillna(0)
    describe = df.describe()
    mean = describe.loc['mean'][0]
    std = describe.loc['std'][0]
    skew = df['E'].skew()
    df_hours = df.groupby(by=df['AESTTime'].dt.hour).sum()
    df_days = df.groupby(by=df['AESTTime'].dt.date).sum()
    diff = df['AESTTime'][1]-df['AESTTime'][0]
    if diff.seconds == 1800:
        df_hours['interval'] = df_hours['E']/730
    else:
        df_hours['interval'] = df_hours['E']/1460
    df_hours_json = df_hours.to_json()
    df_days_json = df_days.to_json()
    return{
        'hours':df_hours_json,
        'days':df_days_json,
        'mean':mean,
        'std':std,
        'skew':skew
        }