from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # <-- We imported the Cloud Claw!
from datetime import datetime
import re

with DAG(
    dag_id="validate_and_reject",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    @task
    def scan_and_sort_files():
        print("Starting Airflow Cloud Validator...")
        
        # 1. We activate the Cloud Claw. 
        # (The company will provide the real 'aws_conn_id' in your access sheet)
        s3 = S3Hook(aws_conn_id="minio_default")
        
        # 2. We define our Cloud Bucket and Folder exactly as the assessment requested
        bucket_name = "crushftp"
        landing_folder = "mediation/landing/"
        
        # 3. s3.list_keys() is the cloud version of os.listdir()!
        # It asks the claw to grab a list of every file sitting in the landing folder
        files = s3.list_keys(bucket_name=bucket_name, prefix=landing_folder)
        
        # If the folder is empty, the claw returns 'None'
        if files is None:
            print("The landing folder is completely empty. Nothing to do!")
            return
            
        print(f"Found files to process: {files}")

    # We trigger the step
    validate_step = scan_and_sort_files()