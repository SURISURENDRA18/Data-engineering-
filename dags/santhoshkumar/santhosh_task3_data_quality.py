from datetime import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from minio import Minio
import pymysql
import pandas as pd
import io

SCHEMA = "santhosh_retail_analytics"
BUCKET = "test"
PREFIX = "santhoshkumar_bad_data"


def get_doris_conn():
    conn_info = BaseHook.get_connection("santhosh_doris")
    conn = pymysql.connect(
        host=conn_info.host,
        port=int(conn_info.port),
        user=conn_info.login,
        password=conn_info.password or "",
        autocommit=True,
    )
    with conn.cursor() as cur:
        cur.execute(f"USE `{SCHEMA}`;")
    return conn


def get_minio_client():
    conn_info = BaseHook.get_connection("santhosh_minio")
    endpoint = f"{conn_info.host}:{conn_info.port}".replace("http://", "").replace("https://", "")
    return Minio(
        endpoint=endpoint,
        access_key=conn_info.login,
        secret_key=conn_info.password,
        secure=False,
    )


@dag(
    dag_id="santhosh_task3_data_quality",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["santhosh", "task3", "data_quality"],
)
def task3_data_quality_dag():

    @task
    def load_bad_data_to_staging():
        client = get_minio_client()
        conn = get_doris_conn()
        try:
            # 1. Download bad CSVs
            cust_obj = client.get_object(BUCKET, f"{PREFIX}/customers_bad.csv")
            cust_df = pd.read_csv(io.BytesIO(cust_obj.read()))

            ord_obj = client.get_object(BUCKET, f"{PREFIX}/orders_bad.csv")
            ord_df = pd.read_csv(io.BytesIO(ord_obj.read()))

            with conn.cursor() as cur:
                # 2. Reset staging tables for bad data test
                cur.execute("CREATE TABLE IF NOT EXISTS stg_customers_bad LIKE stg_customers;")
                cur.execute("TRUNCATE TABLE stg_customers_bad;")
                cur.execute("CREATE TABLE IF NOT EXISTS stg_orders_bad LIKE stg_orders;")
                cur.execute("TRUNCATE TABLE stg_orders_bad;")

                # 3. Load customers
                for _, row in cust_df.iterrows():
                    cid = f"'{row['customer_id']}'" if pd.notna(row['customer_id']) else "NULL"
                    cname = f"'{row['customer_name']}'" if pd.notna(row['customer_name']) else "NULL"
                    city = f"'{row['city']}'" if pd.notna(row['city']) else "NULL"
                    email = f"'{row['email']}'" if pd.notna(row['email']) else "NULL"
                    cdate = f"'{row['created_date']}'" if pd.notna(row['created_date']) else "NULL"
                    cur.execute(f"""
                        INSERT INTO stg_customers_bad (customer_id, customer_name, city, email, created_date)
                        VALUES ({cid}, {cname}, {city}, {email}, {cdate});
                    """)

                # 4. Load orders
                for _, row in ord_df.iterrows():
                    oid = f"'{row['order_id']}'" if pd.notna(row['order_id']) else "NULL"
                    cid = f"'{row['customer_id']}'" if pd.notna(row['customer_id']) else "NULL"
                    odate = f"'{row['order_date']}'" if pd.notna(row['order_date']) else "NULL"
                    qty = row['quantity'] if pd.notna(row['quantity']) else "NULL"
                    price = row['unit_price'] if pd.notna(row['unit_price']) else "NULL"
                    cur.execute(f"""
                        INSERT INTO stg_orders_bad (order_id, customer_id, order_date, quantity, unit_price)
                        VALUES ({oid}, {cid}, {odate}, {qty}, {price});
                    """)

                print("Successfully loaded bad test records into stg_customers_bad and stg_orders_bad.")
        finally:
            conn.close()

    @task
    def run_data_quality_checks():
        conn = get_doris_conn()
        failures = []

        try:
            with conn.cursor() as cur:
                # Check 1: Mandatory IDs (NULL checks)
                cur.execute("SELECT COUNT(*) FROM stg_customers_bad WHERE customer_id IS NULL OR customer_id = '';")
                null_cust = cur.fetchone()[0]
                if null_cust > 0:
                    failures.append(f"[Check 1 FAIL] Found {null_cust} records with missing/NULL customer_id.")

                cur.execute("SELECT COUNT(*) FROM stg_orders_bad WHERE order_id IS NULL OR order_id = '';")
                null_ord = cur.fetchone()[0]
                if null_ord > 0:
                    failures.append(f"[Check 1 FAIL] Found {null_ord} records with missing/NULL order_id.")

                # Check 2: Uniqueness
                cur.execute("""
                    SELECT customer_id, COUNT(*) 
                    FROM stg_customers_bad 
                    WHERE customer_id IS NOT NULL 
                    GROUP BY customer_id HAVING COUNT(*) > 1;
                """)
                dup_cust = cur.fetchall()
                if dup_cust:
                    failures.append(f"[Check 2 FAIL] Found duplicate customer_ids: {dup_cust}")

                # Check 3: Quantity > 0
                cur.execute("SELECT COUNT(*) FROM stg_orders_bad WHERE quantity <= 0;")
                invalid_qty = cur.fetchone()[0]
                if invalid_qty > 0:
                    failures.append(f"[Check 3 FAIL] Found {invalid_qty} records with quantity <= 0.")

                # Check 4: Unit Price Non-Negative
                cur.execute("SELECT COUNT(*) FROM stg_orders_bad WHERE unit_price < 0;")
                neg_price = cur.fetchone()[0]
                if neg_price > 0:
                    failures.append(f"[Check 4 FAIL] Found {neg_price} records with unit_price < 0.")

                # Check 5: Referential Integrity (Orphan Orders)
                cur.execute("""
                    SELECT o.order_id, o.customer_id 
                    FROM stg_orders_bad o 
                    LEFT JOIN stg_customers_bad c ON o.customer_id = c.customer_id 
                    WHERE c.customer_id IS NULL;
                """)
                orphans = cur.fetchall()
                if orphans:
                    failures.append(f"[Check 5 FAIL] Referential Integrity violation! Orders with non-existent customers: {orphans}")

                # Check 6: Row Count Check
                cur.execute("SELECT COUNT(*) FROM stg_orders_bad;")
                total_orders = cur.fetchone()[0]
                if total_orders == 0:
                    failures.append("[Check 6 FAIL] Zero records present in stg_orders_bad.")

        finally:
            conn.close()

        # Print all detected failures to Airflow task logs
        print("\n================ DATA QUALITY AUDIT REPORT ================")
        for fail in failures:
            print(fail)
        print("===========================================================\n")

        # Raise exception to fail the task in Airflow as required by Task 3
        if failures:
            raise AirflowException(f"Data Quality Gate Failed: Detected {len(failures)} critical data anomalies.")

    load_bad_data_to_staging() >> run_data_quality_checks()


task3_data_quality_dag()