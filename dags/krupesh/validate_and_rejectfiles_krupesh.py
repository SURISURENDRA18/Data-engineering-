from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import re

# 1. THE DAG MANAGER
with DAG(
    dag_id="krupesh_validate_and_reject",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    # 2. THE TASK
    @task
    def scan_and_sort_files():
        import gzip
        print("Starting Airflow Cloud Validator...")
        
        s3 = S3Hook(aws_conn_id="krupesh_minio")
        bucket = "crushftp"
        landing_folder = "mediation/landing/"
        
        # --- REAL MEMORY: BUILD HISTORY FROM CLOUD FOLDERS ---
        processed_history = []
        highest_seq = {}
        
        # 1. Ask Cloud Claw to list everything currently inside your validated and rejected folders
        # The 'or []' part is a safety net: if the folder is completely empty, give us an empty list instead of crashing.
        validated_files = s3.list_keys(bucket_name=bucket, prefix="mediation/krupesh/validated/") or []
        rejected_files = s3.list_keys(bucket_name=bucket, prefix="mediation/krupesh/rejected/") or []
        
        # 2. Add all these past files to our processed_history list (Rule 7)
        for full_path in validated_files + rejected_files:
            past_file = full_path.split('/')[-1]
            if past_file:
                processed_history.append(past_file)
                
        # 3. Find the highest sequence number from the previously validated files (Rule 8)
        for full_path in validated_files:
            past_file = full_path.split('/')[-1]
            name_parts = past_file.split('_')
            
            # Make sure it looks like a valid file before we try to read its sequence number
            if len(name_parts) >= 5:
                region = name_parts[0]
                try:
                    seq_num = int(name_parts[3])
                    # If this region is brand new to us, OR if this sequence number is the biggest we've seen, save it!
                    if region not in highest_seq or seq_num > highest_seq[region]:
                        highest_seq[region] = seq_num
                except ValueError:
                    pass # Ignore it if the sequence wasn't a real number
                    
        print(f"Memory loaded! Found {len(processed_history)} past files.")
        print(f"Highest sequences on file: {highest_seq}")
        # -----------------------------------------------------
        
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

        # Get all files in the landing folder
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
                    file_obj = s3.get_key(key=full_cloud_path, bucket_name=bucket)
                    raw_bytes = file_obj.get()['Body'].read()
                    
                    # 2. Check if it's zipped. If yes, unzip it in memory!
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
                            print(f"--- HEADER MISMATCH IN {file_name} ---")
                            print(f"File has {len(actual_headers)} columns, we expect {len(expected_headers)}")
                            
                            # If the counts are the same, let's find the exact typo
                            if len(actual_headers) == len(expected_headers):
                                for i in range(len(actual_headers)):
                                    if actual_headers[i] != expected_headers[i]:
                                        print(f"Mistake at Column {i}:")
                                        print(f"   File has: '{actual_headers[i]}'")
                                        print(f"   We expect: '{expected_headers[i]}'")
                    
                    if failure_reason is None:
                        expected_field_count = len(actual_headers)
                        
                        # Added enumerate so we know exactly which line number fails
                        for line_num, row in enumerate(lines[1:], start=2):
                            if not row.strip():
                                continue
                                
                            row_columns = row.strip().split(';')
                            
                            if len(row_columns) != expected_field_count:
                                failure_reason = "invalid_column_count"
                                
                                # THE DETECTIVE REPORT FOR ROWS:
                                print(f"--- BAD ROW IN {file_name} ---")
                                print(f"Line number {line_num} has {len(row_columns)} columns, but we need {expected_field_count}.")
                                # Print the first 100 characters of the bad row so we can see what's wrong
                                print(f"Look at the data: {row.strip()[:100]}...")
                                break
                                
                except Exception as e:
                    print(f"CRASH REPORT for {file_name}: {e}")
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

            # --- ROUTING TO KRUPESH FOLDERS ---
            if failure_reason is None:
                print("Result: PASSED! Moving to validated folder.")
                destination_path = f"mediation/krupesh/validated/{file_name}"
            else:
                print(f"Result: FAILED ({failure_reason}). Moving to rejected folder.")
                destination_path = f"mediation/krupesh/rejected/{failure_reason}/{file_name}"

            # Step A: Copy to new location
            s3.copy_object(
                source_bucket_key=full_cloud_path,
                dest_bucket_key=destination_path,
                source_bucket_name=bucket,
                dest_bucket_name=bucket
            )
           

        print("\nAll files processed successfully!")

    # 3. TRIGGER THE TASK
    validate_step = scan_and_sort_files()

    
    # ---------------------------------------------------------
    # TASK 2: LOAD TO APACHE DORIS
    # ---------------------------------------------------------
    @task
    def load_validated_to_doris():
        print("Starting Doris Raw Table Loader...")
        
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        import csv
        import io
        import gzip
        
        s3 = S3Hook(aws_conn_id="krupesh_minio")
        bucket = "crushftp"
        
        # We assume you have a connection set up in Airflow for Doris
        # Doris uses the MySQL protocol, so MySqlHook works perfectly!
        doris_hook = MySqlHook(mysql_conn_id="krupesh_doris")
        
        # 1. Create the Table in Doris (if it doesn't exist)
        # We use VARCHAR for everything in the RAW layer to prevent load crashes.
        # Data types are usually cast in the Silver/Gold layers later.
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS actives_raw (
            CompanyID VARCHAR(255), CompanyName VARCHAR(255), InvoiceNumber VARCHAR(255),
            BusinessUnitName VARCHAR(255), DepartmentName VARCHAR(255), CostCenterName VARCHAR(255),
            CommercialOfferName VARCHAR(255), BusinessUnitLevel VARCHAR(255), BusinessUnitTAG VARCHAR(255),
            SharedBalanceUsed VARCHAR(255), AccountNumber VARCHAR(255), CustomerNumber VARCHAR(255),
            MSISDN VARCHAR(255), ProductStatus VARCHAR(255), ProvProductType VARCHAR(255),
            ActivationDate VARCHAR(255), LastProductStatusChangeDate VARCHAR(255),
            CommercialOfferDescription VARCHAR(255), LastTrafficDate VARCHAR(255), ProductID VARCHAR(255),
            ICCID VARCHAR(255), ExtraMSISDN_1 VARCHAR(255), ExtraCreationDate_1 VARCHAR(255),
            ExtraMSISDN_2 VARCHAR(255), ExtraCreationDate_2 VARCHAR(255), ExtraMSISDN_3 VARCHAR(255),
            ExtraCreationDate_3 VARCHAR(255), ExtraMSISDN_4 VARCHAR(255), ExtraCreationDate_4 VARCHAR(255),
            PlanName VARCHAR(255), PlanStartDate VARCHAR(255), Carrier_Name VARCHAR(255),
            CDP_Platform VARCHAR(255), Carrier_AccountNo VARCHAR(255), ProviderName VARCHAR(255),
            Type VARCHAR(255), Profile_version VARCHAR(255), Rate_Plan_Name VARCHAR(255),
            Rate_Plan_Code_ID VARCHAR(255), Zone_Type VARCHAR(255), Whitelisted_Zones VARCHAR(255),
            Blacklisted_Zones VARCHAR(255), SIM_Batch VARCHAR(255), SIM_Vendor VARCHAR(255),
            Sim_Type VARCHAR(255), EID VARCHAR(255)
        ) ENGINE=OLAP
        DUPLICATE KEY(CompanyID, InvoiceNumber, MSISDN)
        DISTRIBUTED BY HASH(MSISDN) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
        
        doris_hook.run(create_table_sql)
        print("Doris table 'actives_raw' is ready.")

        # 2. Find Validated Files
        validated_prefix = "mediation/krupesh/validated/"
        archive_prefix = "mediation/krupesh/archive/"
        
        validated_files = s3.list_keys(bucket_name=bucket, prefix=validated_prefix)
        
        if not validated_files:
            print("No new validated files to load into Doris.")
            return

        # 3. Process and Load Each File
        for full_cloud_path in validated_files:
            file_name = full_cloud_path.split('/')[-1]
            if not file_name or file_name == "validated":
                continue
                
            print(f"Loading {file_name} into Doris...")
            
            # Read and unzip
            file_obj = s3.get_key(key=full_cloud_path, bucket_name=bucket)
            raw_bytes = file_obj.get()['Body'].read()
            
            if file_name.endswith(".gz"):
                file_text = gzip.decompress(raw_bytes).decode("utf-8-sig")
            else:
                file_text = raw_bytes.decode("utf-8-sig")
                
            reader = csv.reader(io.StringIO(file_text), delimiter=';')
            
            try:
                next(reader) # Skip the header row
            except StopIteration:
                continue # Empty file
                
            # Prepare rows for batch insert
            rows_to_insert = []
            for row in reader:
                if row:
                    rows_to_insert.append(tuple(row))
            
            # Batch Insert into Doris
            if rows_to_insert:
                doris_hook.insert_rows(
                    table="actives_raw",
                    rows=rows_to_insert,
                    target_fields=None, # Auto-maps because we have exactly 46 columns
                    commit_every=5000   # Batch size to prevent memory crashes
                )
                print(f"Successfully loaded {len(rows_to_insert)} rows into Doris!")
            
            # 4. Move to Archive so it doesn't get loaded again tomorrow
            archive_key = f"{archive_prefix}{file_name}"
            s3.copy_object(
                source_bucket_key=full_cloud_path,
                dest_bucket_key=archive_key,
                source_bucket_name=bucket,
                dest_bucket_name=bucket
            )
            # Delete from validated so it's clean for the next run
            s3.delete_object(bucket_name=bucket, key=full_cloud_path)
            print(f"Archived {file_name}.")

    load_step = load_validated_to_doris()

    # ---------------------------------------------------------
    # DAG DEPENDENCIES (This tells Airflow what order to run in)
    # ---------------------------------------------------------
    validate_step >> load_step