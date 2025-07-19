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
        state = glue.get_crawler(Name=crawler_name)['Crawler']['State']
        if state == 'READY':
            glue.start_crawler(Name=crawler_name)
            print(f"Started crawler: {crawler_name}")
            break
        else:
            print(f"Crawler {crawler_name} is {state}, waiting 10s...")
            time.sleep(10)

def is_job_running(job_name):
    runs = glue.get_job_runs(JobName=job_name, MaxResults=5)
    for run in runs.get('JobRuns', []):
        if run['JobRunState'] in ['RUNNING', 'STARTING', 'STOPPING']:
            print(f"Job {job_name} is currently {run['JobRunState']}.")
            return True
    return False

def wait_for_job_completion(job_name, run_id):
    while True:
        run = glue.get_job_run(JobName=job_name, RunId=run_id)
        status = run['JobRun']['JobRunState']
        if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
            print(f"Job {job_name} completed with status: {status}")
            return status
        else:
            print(f"Job {job_name} is still {status}, waiting 15s...")
            time.sleep(15)

def lambda_handler(event, context):
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data'])
        data = json.loads(payload)

        # Save to Bronze Layer in S3
        now = datetime.utcnow()
        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')
        timestamp = now.strftime('%Y-%m-%d-%H-%M-%S-%f')
        s3_key = f"Bronze_Layer/year={year}/month={month}/day={day}/{timestamp}.json"

        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=json.dumps(data))
        print(f"Data written to {s3_key}")

    # Step 1: Start Bronze Crawler
    start_crawler_when_idle('bronze-crawler')

    # Step 2: Start Silver Job (if not already running)
    if not is_job_running('sil_job'):
        silver_response = glue.start_job_run(JobName='sil_job')
        silver_run_id = silver_response['JobRunId']
        print(f"Silver Glue Job started with Run ID: {silver_run_id}")
        wait_for_job_completion('sil_job', silver_run_id)
    else:
        print("Silver Glue Job already running. Skipping.")

    # Step 3: Start Gold Job (if not already running)
    if not is_job_running('gold_job'):
        gold_response = glue.start_job_run(JobName='gold_job')
        gold_run_id = gold_response['JobRunId']
        print(f"Gold Glue Job started with Run ID: {gold_run_id}")
        wait_for_job_completion('gold_job', gold_run_id)
    else:
        print("Gold Glue Job already running. Skipping.")

    # Step 4: Start Gold Crawler
    start_crawler_when_idle('gold_crawler')

    return {
        'statusCode': 200,
        'body': 'Bronze to Gold pipeline executed successfully.'
    }
