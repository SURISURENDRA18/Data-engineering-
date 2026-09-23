from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import re

# 1. THE DAG MANAGER
with DAG(
    dag_id="validate_and_reject",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    # 2. THE TASK
    @task
    def scan_and_sort_files():
        print("Starting Airflow Cloud Validator...")
        
        s3 = S3Hook(aws_conn_id="krupesh_minio")
        bucket = "crushftp"
        landing_folder = "mediation/landing/"
        
        # Fake memory for Rules 7 & 8 
        processed_history = ["HOL_01_Active_000123455_20260920050000.csv"]
        highest_seq = {"HOL": 123455}
        
        expected_headers = [
            "CompanyID", "CompanyName", "Invoice Number", "BusinessUnitName", "DepartmentName",
            "CostCenterName", "Commercial OfferName", "BusinessUnitLevel", "Business Unit TAG",
            "Shared Balance Used", "AccountNumber", "CustomerNumber", "MSISDN", "ProductStatus",
            "ProvProductType", "Activation Date", "LastProductStatusChangeDate",
            "Commercial OfferDescription", "Last TrafficDate", "ProductID", "ICCID",
            "ExtraMSISDN_1", "ExtraCreation Date_1", "ExtraMSISDN_2", "ExtraCreationDate_2",
            "ExtraMSISDN_3", "ExtraCreationDate_3", "ExtraMSISDN 4", "ExtraCreationDate_4",
            "PlanName", "PlanStartDate", "Carrier_Name", "CDP Platform", "Carrier_AccountNo",
            "ProviderName", "Type", "Profile_version", "Rate_Plan_Name", "Rate_Plan_Code_ID",
            "Zone_Type", "Whitelisted Zones", "Blacklisted_Zones", "SIM_Batch", "SIM_Vendor",
            "Sim_Type", "EID"
        ]

        # Get all files in the cloud folder
        cloud_files = s3.list_keys(bucket_name=bucket, prefix=landing_folder)
        
        if not cloud_files:
            print("Landing folder is empty. Nothing to do!")
            return

        for full_cloud_path in cloud_files:
            file_name = full_cloud_path.split('/')[-1]
            
            if file_name == "" or file_name == "landing":
                continue
                
            print(f"\n--- Checking: {file_name} ---")
            
            failure_reason = None
            
            # RULE 1: Pattern
            expected_pattern = r"^[A-Za-z]+_[0-9]+_(Active|Actives)_[0-9]+_[0-9]+\.csv(\.gz)?$"
            if not re.match(expected_pattern, file_name):
                failure_reason = "pattern_mismatch"
                
            # RULE 2: Size
            if failure_reason is None:
                file_obj = s3.get_key(key=full_cloud_path, bucket_name=bucket)
                if file_obj.content_length == 0:
                    failure_reason = "zero_size"

            # RULES 4, 3, 5, 6
            if failure_reason is None:
                try:
                    file_text = s3.read_key(key=full_cloud_path, bucket_name=bucket)
                    lines = file_text.splitlines()
                    
                    if len(lines) < 2:
                        failure_reason = "header_only"
                    
                    if failure_reason is None:
                        actual_headers = lines[0].strip().split(';')
                        if len(actual_headers) < 9 or actual_headers != expected_headers:
                            failure_reason = "invalid_column_count"
                    
                    if failure_reason is None:
                        expected_field_count = len(actual_headers)
                        for row in lines[1:]:
                            if not row.strip():
                                continue
                            if len(row.strip().split(';')) != expected_field_count:
                                failure_reason = "invalid_column_count"
                                break
                                
                except Exception:
                    failure_reason = "not_text_format"

            # RULE 7: Duplicate
            if failure_reason is None and file_name in processed_history:
                failure_reason = "duplicate"
                
            # RULE 8: Sequence
            if failure_reason is None:
                name_parts = file_name.split('_')
                region = name_parts[0]
                seq_num = int(name_parts[3])
                
                if region in highest_seq and seq_num <= highest_seq[region]:
                    failure_reason = "sequence_invalid"

            # --- UPDATED PATHS FOR KRUPESH ---
            if failure_reason is None:
                print("Result: PASSED! Moving to validated folder.")
                # Notice we added /krupesh/ here!
                destination_path = f"mediation/krupesh/validated/{file_name}"
            else:
                print(f"Result: FAILED ({failure_reason}). Moving to rejected folder.")
                # Notice we added /krupesh/ here too!
                destination_path = f"mediation/krupesh/rejected/{failure_reason}/{file_name}"

            # Step A: Copy to new location
            s3.copy_object(
                source_bucket_key=full_cloud_path,
                dest_bucket_key=destination_path,
                source_bucket_name=bucket,
                dest_bucket_name=bucket
            )
            # Step B: Delete from landing location
            #s3.delete_object(bucket_name=bucket, key=full_cloud_path)

        print("\nAll files processed successfully!")

    # 3. TRIGGER THE TASK
    validate_step = scan_and_sort_files()