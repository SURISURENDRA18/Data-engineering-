from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import re
import gzip
import io

# 1. THE DAG MANAGER
with DAG(
    dag_id="krupesh_validate_and_reject_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    # 2. THE TASK
    @task
    def scan_and_sort_files():
        print("Starting Airflow Cloud Validator...")
        
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
        
        validated_files = s3.list_keys(bucket_name=bucket, prefix="mediation/krupesh/validated/") or []
        rejected_files = s3.list_keys(bucket_name=bucket, prefix="mediation/krupesh/rejected/") or []
        
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

            lines = []
            if failure_reason is None:
                try:
                    if file_name.endswith(".gz"):
                        file_text = gzip.decompress(raw_bytes).decode("utf-8-sig")
                    else:
                        file_text = raw_bytes.decode("utf-8-sig")
                        
                    lines = file_text.splitlines()
                    
                    if len(lines) < 2:
                        failure_reason = "header_only"
                    
                    if failure_reason is None:
                        actual_headers = [col.strip().strip('"') for col in lines[0].strip().split(';')]
                        if len(actual_headers) < 9 or actual_headers != expected_headers:
                            failure_reason = "invalid_column_count"
                    
                    # RULE 6: THE FRIEND'S 21-COLUMN LOGIC
                    if failure_reason is None:
                        expected_field_count = len(actual_headers)
                        for row in lines[1:]:
                            if not row.strip(): continue
                            field_count = len(row.strip().split(';'))
                            # If it drops below 21 (mandatory) or goes over 46, fail it!
                            if field_count < 21 or field_count > expected_field_count:
                                failure_reason = "invalid_column_count"
                                break
                                
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

            # --- ROUTING & RESILIENCE LOGIC ---
            if failure_reason is None:
                s3.copy_object(
                    source_bucket_key=full_cloud_path,
                    dest_bucket_key=f"mediation/krupesh/validated/{file_name}",
                    source_bucket_name=bucket, dest_bucket_name=bucket
                )
                print("Result: PASSED. Copied to validated.")
                
            else:
                s3.copy_object(
                    source_bucket_key=full_cloud_path,
                    dest_bucket_key=f"mediation/krupesh/rejected/{failure_reason}/{file_name}",
                    source_bucket_name=bucket, dest_bucket_name=bucket
                )
                print(f"Result: FAILED ({failure_reason}). Copied to rejected.")
                
                # THE FRIEND'S SURGERY: Pad missing columns up to 46!
                if failure_reason == "invalid_column_count" and lines:
                    expected_field_count = len(expected_headers)
                    good_rows = [lines[0]] 
                    rescued_count = 0
                    
                    for row in lines[1:]:
                        if not row.strip(): continue
                        columns = row.strip().split(';')
                        
                        # If row has 21 to 45 columns, pad it with blanks!
                        if 21 <= len(columns) <= expected_field_count:
                            if len(columns) < expected_field_count:
                                # Add empty strings to reach exactly 46
                                columns = columns + [""] * (expected_field_count - len(columns))
                            
                            good_rows.append(";".join(columns))
                            rescued_count += 1
                    
                    if rescued_count > 0:
                        cleaned_text = "\n".join(good_rows) + "\n"
                        out_buf = io.BytesIO()
                        with gzip.GzipFile(fileobj=out_buf, mode="wb") as gz:
                            gz.write(cleaned_text.encode("utf-8"))
                            
                        s3.load_bytes(
                            bytes_data=out_buf.getvalue(),
                            key=f"mediation/krupesh/validated/{file_name}",
                            bucket_name=bucket, replace=True
                        )
                        print(f"SURGERY SUCCESS: Padded and rescued {rescued_count} rows!")

    validate_step = scan_and_sort_files()