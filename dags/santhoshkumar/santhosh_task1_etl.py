import csv
import io
from datetime import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from minio import Minio
import pymysql

# MinIO & Doris configuration
BUCKET_NAME = "test"
CUSTOMERS_FILE = "santhosh_raw_data/customers.csv"
ORDERS_FILE = "santhosh_raw_data/orders.csv"
DEST_SCHEMA = "santhosh_retail_analytics"


def get_minio_client():
    conn = BaseHook.get_connection("minio_default")
    return Minio(
        f"{conn.host}:{conn.port}",
        access_key=conn.login,
        secret_key=conn.password,
        secure=False,
    )


def get_doris_connection():
    doris_conn = BaseHook.get_connection("doris_default")
    return pymysql.connect(
        host=doris_conn.host,
        port=int(doris_conn.port),
        user=doris_conn.login,
        password=doris_conn.password or "",
        database=DEST_SCHEMA,
        autocommit=True,
    )


@dag(
    dag_id="santhosh_task1_etl",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["santhosh", "task1", "etl"],
)
def task1_etl_pipeline():

    @task
    def check_input_files():
        client = get_minio_client()
        for f in [CUSTOMERS_FILE, ORDERS_FILE]:
            client.stat_object(BUCKET_NAME, f)
            print(f"Found file: {f}")
        return True

    @task
    def load_customers_staging():
        client = get_minio_client()
        response = client.get_object(BUCKET_NAME, CUSTOMERS_FILE)
        content = response.read().decode("utf-8")
        reader = list(csv.DictReader(io.StringIO(content)))

        conn = get_doris_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE stg_customers;")
                insert_sql = """
                    INSERT INTO stg_customers (customer_id, customer_name, city, email, created_date)
                    VALUES (%s, %s, %s, %s, %s)
                """
                records = [
                    (
                        r["customer_id"],
                        r["customer_name"],
                        r["city"],
                        r["email"],
                        r["created_date"],
                    )
                    for r in reader
                ]
                cur.executemany(insert_sql, records)
                print(f"Loaded {len(records)} records into stg_customers.")
        finally:
            conn.close()

    @task
    def load_orders_staging():
        client = get_minio_client()
        response = client.get_object(BUCKET_NAME, ORDERS_FILE)
        content = response.read().decode("utf-8")
        reader = list(csv.DictReader(io.StringIO(content)))

        conn = get_doris_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE stg_orders;")
                insert_sql = """
                    INSERT INTO stg_orders (order_id, customer_id, order_date, quantity, unit_price)
                    VALUES (%s, %s, %s, %s, %s)
                """
                records = [
                    (
                        r["order_id"],
                        r["customer_id"],
                        r["order_date"],
                        r["quantity"],
                        r["unit_price"],
                    )
                    for r in reader
                ]
                cur.executemany(insert_sql, records)
                print(f"Loaded {len(records)} records into stg_orders.")
        finally:
            conn.close()

    @task
    def transform_and_load_target():
        conn = get_doris_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE orders_transformed;")
                transform_sql = """
                    INSERT INTO orders_transformed (order_id, customer_id, order_date, quantity, unit_price, total_amount)
                    SELECT 
                        order_id,
                        customer_id,
                        order_date,
                        quantity,
                        unit_price,
                        (quantity * unit_price) AS total_amount
                    FROM stg_orders;
                """
                cur.execute(transform_sql)
                print("Loaded transformed records into orders_transformed.")
        finally:
            conn.close()

    @task
    def validate_load():
        conn = get_doris_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM orders_transformed;")
                cnt = cur.fetchone()[0]
                if cnt == 0:
                    raise ValueError("Validation failed: orders_transformed is empty!")
                print(f"Validation successful: {cnt} records in orders_transformed.")
        finally:
            conn.close()

    # Flow
    chk = check_input_files()
    c_stg = load_customers_staging()
    o_stg = load_orders_staging()
    trans = transform_and_load_target()
    val = validate_load()

    chk >> [c_stg, o_stg] >> trans >> val


task1_etl_pipeline()