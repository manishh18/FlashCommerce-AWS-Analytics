import boto3
import json
import base64
import time
from datetime import datetime
s3 = boto3.client('s3')
glue = boto3.client('glue')
bucket_name = "e-com-lake"
def crawler_exists(crawler_name):
try:
glue.get_crawler(Name=crawler_name)
return True
except glue.exceptions.EntityNotFoundException:
return False
def start_crawler_when_idle(crawler_name):
if not crawler_exists(crawler_name):
print(f"Crawler {crawler_name} does not exist. Skipping start.")
return
while True:
status = glue.get_crawler(Name=crawler_name)['Crawler']['State']
if status == 'READY':
glue.start_crawler(Name=crawler_name)
print(f"Started crawler: {crawler_name}")
break
else:
print(f"Crawler {crawler_name} is {status}, waiting 10s...")
time.sleep(10)
def lambda_handler(event, context):
for record in event['Records']:
payload = base64.b64decode(record['kinesis']['data'])
data = json.loads(payload)
# Generate timestamped path in S3
now = datetime.utcnow()
year = now.strftime('%Y')
month = now.strftime('%m')
day = now.strftime('%d')
timestamp = now.strftime('%Y-%m-%d-%H-%M-%S-%f')
s3_key = f"Bronze_Layer/year={year}/month={month}/day={day}/{timestamp}.json"
# Upload to Bronze Layer
s3.put_object(Bucket=bucket_name, Key=s3_key, Body=json.dumps(data))
print(f"Data written to {s3_key}")
# Step 1: Start Bronze Crawler
start_crawler_when_idle('bronze-crawler')
# Step 2: Start Silver Job
glue.start_job_run(JobName='sil_job')
print("Silver Glue Job started")
# Step 3: Start Gold Job
glue.start_job_run(JobName='gold_job')
print("Gold Glue Job started")
# Step 4: Start Gold Crawler (correct name with underscore)
start_crawler_when_idle('gold_crawler')
return {
'statusCode': 200,
'body': 'Bronze to Gold pipeline executed successfully.'
}
