from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import re
import gzip
import csv
import io

# 1. THE DAG MANAGER
with DAG(
    dag_id="krupesh_validate_and_reject_strict",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    # 2. THE TASK
    @task
    def scan_and_sort_files():
        print("Starting Strict Airflow Cloud Validator...")
        
        s3 = S3Hook(aws_conn_id="krupesh_minio")
        bucket = "crushftp"
        
        expected_headers = [
            "CompanyID", "CompanyName", "InvoiceNumber", "BusinessUnitName", "DepartmentName",
            "CostCenterName", "CommercialOfferName", "BusinessUnitLevel", "BusinessUnitTAG",
            "SharedBalanceUsed", "AccountNumber", "CustomerNumber", "MSISDN", "ProductStatus",
            "ProvProductType", "ActivationDate", "LastProductStatusChangeDate",
            "CommercialOfferDescription", "LastTrafficDate", "ProductID", "ICCID",
            "ExtraMSISDN_1", "ExtraCreationDate_1", "ExtraMSISDN_2", "ExtraCreationDate_2",
            "ExtraMSISDN_3", "ExtraCreationDate_3", "ExtraMSISDN_4", "ExtraCreationDate_4",
            "PlanName", "PlanStartDate", "Carrier_Name", "CDP_Platform", "Carrier_AccountNo",
            "ProviderName", "Type", "Profile_version", "Rate_Plan_Name", "Rate_Plan_Code_ID",
            "Zone_Type", "Whitelisted_Zones", "Blacklisted_Zones", "SIM_Batch", "SIM_Vendor",
            "Sim_Type", "EID"
        ]

        # --- REAL MEMORY: BUILD HISTORY ---
        processed_history = []
        highest_seq = {}
        
        validated_files = s3.list_keys(bucket_name=bucket, prefix="mediation/krupesh2/validated/") or []
        rejected_files = s3.list_keys(bucket_name=bucket, prefix="mediation/krupesh2/rejected/") or []
        
        for full_path in validated_files + rejected_files:
            past_file = full_path.split('/')[-1]
            if past_file:
                processed_history.append(past_file)
                
        for full_path in validated_files:
            past_file = full_path.split('/')[-1]
            name_parts = past_file.split('_')
            if len(name_parts) >= 5:
                region = name_parts[0]
                try:
                    seq_num = int(name_parts[3])
                    if region not in highest_seq or seq_num > highest_seq[region]:
                        highest_seq[region] = seq_num
                except ValueError:
                    pass
        # ----------------------------------

        cloud_files = s3.list_keys(bucket_name=bucket, prefix="mediation/landing/")
        if not cloud_files:
            print("Landing folder is empty. Nothing to do!")
            return

        for full_cloud_path in cloud_files:
            file_name = full_cloud_path.split('/')[-1]
            
            if file_name == "" or file_name == "landing" or "Active" not in file_name:
                continue
                
            print(f"\n--- Checking: {file_name} ---")
            
            failure_reason = None
            
            file_obj = s3.get_key(key=full_cloud_path, bucket_name=bucket)
            raw_bytes = file_obj.get()['Body'].read()
            
            # RULE 1 & 2
            if not re.match(r"^[A-Za-z]+_[0-9]+_(Active|Actives)_[0-9]+_[0-9]+\.csv(\.gz)?$", file_name):
                failure_reason = "pattern_mismatch"
            elif len(raw_bytes) == 0:
                failure_reason = "zero_size"

            # RULES 3, 4, 5, 6
            if failure_reason is None:
                try:
                    if file_name.endswith(".gz"):
                        file_text = gzip.decompress(raw_bytes).decode("utf-8-sig")
                    else:
                        file_text = raw_bytes.decode("utf-8-sig")
                        
                    # THE FIX: Use csv.reader instead of splitlines() and .split()
                    reader = csv.reader(io.StringIO(file_text), delimiter=';')
                    
                    # Rule 3 & 5: Check Headers
                    try:
                        headers = next(reader)
                        actual_headers = [col.strip().strip('"') for col in headers]
                    except StopIteration:
                        actual_headers = [] # File is empty
                        
                    if len(actual_headers) == 0:
                        failure_reason = "header_only"
                    elif len(actual_headers) < 9 or actual_headers != expected_headers:
                        failure_reason = "invalid_column_count"
                    
                    # Rule 6: Row Check using the smart reader
                    if failure_reason is None:
                        expected_field_count = len(actual_headers)
                        row_count = 0
                        
                        for row in reader:
                            if not row: continue # Skip empty lines
                            row_count += 1
                            
                            # Strict Check: Exactly 46 columns, but now it respects quotes!
                            if len(row) != expected_field_count:
                                failure_reason = "invalid_column_count"
                                break
                                
                        if failure_reason is None and row_count == 0:
                            failure_reason = "header_only"
                                
                except Exception as e:
                    print(f"Decode error: {e}")
                    failure_reason = "not_text_format"
            # RULE 7 & 8
            if failure_reason is None and file_name in processed_history:
                failure_reason = "duplicate"
            elif failure_reason is None:
                name_parts = file_name.split('_')
                if name_parts[0] in highest_seq and int(name_parts[3]) <= highest_seq[name_parts[0]]:
                    failure_reason = "sequence_invalid"

            # --- ROUTING LOGIC (NO SURGERY) ---
            if failure_reason is None:
                # Perfect file -> Validated
                s3.copy_object(
                    source_bucket_key=full_cloud_path,
                    dest_bucket_key=f"mediation/krupesh2/validated/{file_name}",
                    source_bucket_name=bucket, dest_bucket_name=bucket
                )
                print("Result: PASSED. Copied to validated.")
                
            else:
                # Broken file -> Rejected
                s3.copy_object(
                    source_bucket_key=full_cloud_path,
                    dest_bucket_key=f"mediation/krupesh2/rejected/{failure_reason}/{file_name}",
                    source_bucket_name=bucket, dest_bucket_name=bucket
                )
                print(f"Result: FAILED ({failure_reason}). Copied to rejected.")

    validate_step = scan_and_sort_files()
