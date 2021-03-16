import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:19000',
    aws_access_key_id='***',
    aws_secret_access_key='***',
)

r = s3.select_object_content(
    Bucket='feast',
    Key='offline/driver_statistics/1615895988.parquet',
    ExpressionType='SQL',
    Expression="select created_timestamp, event_timestamp, driver_id from s3object limit 1",
    InputSerialization={'Parquet': {}},
    OutputSerialization={'CSV': {}},
)

for event in r['Payload']:
    print(event)
    if 'Records' in event:
        records = event['Records']['Payload'].decode('utf-8')
        print(records)
    elif 'Stats' in event:
        statsDetails = event['Stats']['Details']
        # print("Stats details bytesScanned: ")
        # print(statsDetails['BytesScanned'])
        # print("Stats details bytesProcessed: ")
        # print(statsDetails['BytesProcessed'])
