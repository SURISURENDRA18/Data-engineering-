from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import re
from airflow.providers.mysql.hooks.mysql import MySqlHook
import csv
import io
import gzip


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

    
    # ==========================================
    # TASK 2: LOAD TO APACHE DORIS
    # ==========================================
    @task
    def load_validated_to_doris():
        print("Starting Doris Raw Table Loader with Row-Level Rules...")
        
        s3 = S3Hook(aws_conn_id="krupesh_minio")
        bucket = "crushftp"
        doris_hook = MySqlHook(mysql_conn_id="krupesh_doris")
        
        # 1. TABLE CREATION 
        create_raw_table_sql = """
        CREATE TABLE IF NOT EXISTS raw_actives_daily (
            iccid                      VARCHAR(32) NOT NULL,
            file_name                  VARCHAR(255),
            companyid                  VARCHAR(64),
            companyname                VARCHAR(255),
            invoicenumber              VARCHAR(64),
            businessunitname           VARCHAR(255),
            departmentname             VARCHAR(255),
            costcentername             VARCHAR(255),
            commercialoffername        VARCHAR(255),
            businessunitlevel          VARCHAR(10),
            businessunittag            VARCHAR(255),
            sharedbalanceused          VARCHAR(50),
            accountnumber              VARCHAR(64),
            customernumber             VARCHAR(64),
            msisdn                     VARCHAR(32),
            productstatus              VARCHAR(50),
            provproducttype            VARCHAR(100),
            activationdate             VARCHAR(30),
            lastproductstatuschangedate VARCHAR(30),
            commercialofferdescription VARCHAR(255),
            lasttrafficdate            VARCHAR(30),
            productid                  VARCHAR(64),
            extramsisdn1               VARCHAR(32),
            extracreationdate1         VARCHAR(30),
            extramsisdn2               VARCHAR(32),
            extracreationdate2         VARCHAR(30),
            extramsisdn3               VARCHAR(32),
            extracreationdate3         VARCHAR(30),
            extramsisdn4               VARCHAR(32),
            extracreationdate4         VARCHAR(30),
            planname                   VARCHAR(255),
            planstartdate              VARCHAR(30),
            carriername                VARCHAR(255),
            cdpplatform                VARCHAR(100),
            carrieraccountno           VARCHAR(64),
            providername               VARCHAR(255),
            `type`                     VARCHAR(50),
            profileversion             VARCHAR(50),
            rateplanname               VARCHAR(255),
            rateplancodeid             VARCHAR(64),
            zonetype                   VARCHAR(50),
            whitelistedzones           VARCHAR(500),
            blacklistedzones           VARCHAR(500),
            simbatch                   VARCHAR(100),
            simvendor                  VARCHAR(100),
            simtype                    VARCHAR(50),
            eid                        VARCHAR(64),
            loaded_at                  DATETIME
        )
        UNIQUE KEY(iccid)
        DISTRIBUTED BY HASH(iccid) BUCKETS 8
        PROPERTIES ("replication_num" = "1");
        """
        
        create_reject_table_sql = """
        CREATE TABLE IF NOT EXISTS raw_actives_daily_rejected (
            file_name                  VARCHAR(255),
            companyid                  VARCHAR(64),
            customernumber             VARCHAR(64),
            iccid                      VARCHAR(32),
            reject_reason              VARCHAR(255),
            loaded_at                  DATETIME
        )
        DUPLICATE KEY(file_name)
        DISTRIBUTED BY HASH(file_name) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
        """
        
        doris_hook.run(create_raw_table_sql)
        doris_hook.run(create_reject_table_sql)

        # 2. FILE DISCOVERY 
        validated_prefix = "mediation/krupesh2/validated/"
        archive_prefix = "mediation/krupesh2/archive/"
        validated_files = s3.list_keys(bucket_name=bucket, prefix=validated_prefix)
        
        if not validated_files:
            print("No validated files to load.")
            return

        # 3. PROCESSING (Per-file isolation)
        for full_cloud_path in validated_files:
            file_name = full_cloud_path.split('/')[-1]
            if not file_name or file_name == "validated":
                continue
                
            print(f"\n--- Loading: {file_name} ---")
            
            try:
                file_obj = s3.get_key(key=full_cloud_path, bucket_name=bucket)
                raw_bytes = file_obj.get()['Body'].read()
                
                if file_name.endswith(".gz"):
                    file_text = gzip.decompress(raw_bytes).decode("utf-8-sig")
                else:
                    file_text = raw_bytes.decode("utf-8-sig")
                    
                reader = csv.reader(io.StringIO(file_text), delimiter=';')
                try:
                    next(reader) # Skip Header
                except StopIteration:
                    continue
                
                loaded_at_ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                
                dedup_valid_rows = {} 
                rejected_rows = []
                
                for row in reader:
                    if not row: continue
                    
                    # Pad short rows dynamically to exactly 46 columns
                    expected_csv_cols = 46
                    if len(row) < expected_csv_cols:
                        row = row + [""] * (expected_csv_cols - len(row))
                    
                    companyid = row[0].strip()
                    customernumber = row[11].strip()
                    iccid = row[20].strip()
                    
                    reject_reason = None
                    if not iccid:
                        reject_reason = "NULL_OR_EMPTY_ICCID"
                    elif not customernumber:
                        reject_reason = "NULL_OR_EMPTY_CUSTOMER_NUMBER"
                    elif not companyid or companyid == "0":
                        reject_reason = "NULL_OR_ZERO_COMPANY_ID"
                        
                    if reject_reason:
                        rejected_rows.append((file_name, companyid, customernumber, iccid, reject_reason, loaded_at_ts))
                    else:
                        # Reorder to match DDL: Extract iccid, put it first, then file_name, then the rest
                        row_without_iccid = row[:20] + row[21:]
                        final_row = [iccid, file_name] + row_without_iccid + [loaded_at_ts]
                        dedup_valid_rows[iccid] = final_row
                
                valid_rows_to_insert = list(dedup_valid_rows.values())
                
                # 4. DATABASE INSERTS (High-Speed One-Shot)
                if valid_rows_to_insert or rejected_rows:
                    conn = doris_hook.get_conn()
                    cursor = conn.cursor()
                    try:
                        if valid_rows_to_insert:
                            # 48 placeholders for the 48 columns in target table
                            placeholders = ", ".join(["%s"] * 48)
                            insert_sql = f"INSERT INTO raw_actives_daily VALUES ({placeholders})"
                            cursor.executemany(insert_sql, valid_rows_to_insert)
                        
                        if rejected_rows:
                            # 6 placeholders for the 6 columns in reject table
                            rej_placeholders = ", ".join(["%s"] * 6)
                            rej_sql = f"INSERT INTO raw_actives_daily_rejected VALUES ({rej_placeholders})"
                            cursor.executemany(rej_sql, rejected_rows)
                            
                        conn.commit()
                    finally:
                        cursor.close()
                        conn.close()
                
                # 5. LOGGING RESULTS
                print(f"SUCCESS: {file_name}")
                print(f" -> Rows Loaded: {len(valid_rows_to_insert)}")
                print(f" -> Rows Rejected: {len(rejected_rows)}")
                
                # 6. IDEMPOTENCY EXECUTION (Move to archive)
                s3.copy_object(
                    source_bucket_key=full_cloud_path,
                    dest_bucket_key=f"{archive_prefix}{file_name}",
                    source_bucket_name=bucket, dest_bucket_name=bucket
                )
                s3.delete_object(bucket_name=bucket, key=full_cloud_path)

            except Exception as e:
                print(f"FAILED to process {file_name}. Error: {str(e)}")
                continue

    load_step = load_validated_to_doris()

    # ==========================================
    # DAG DEPENDENCIES
    # ==========================================
    validate_step >> load_step