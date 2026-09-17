from datetime import datetime
import csv
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from minio import Minio
import pymysql

# MinIO & Doris config
BUCKET_NAME = "test"
CUSTOMERS_FILE = "santhosh_raw_data/customers.csv"
ORDERS_FILE = "santhosh_raw_data/orders.csv"
DEST_SCHEMA = "santhosh_retail_analytics"


def fetch_minio_client():
    conn = BaseHook.get_connection("minio_conn")
    return Minio(
        f"{conn.host}:{conn.port}",
        access_key=conn.login,
        secret_key=conn.password,
        secure=False,
    )


def fetch_doris_connection():
    conn = BaseHook.get_connection("doris_conn")
    return pymysql.connect(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password,
        database=DEST_SCHEMA,
        autocommit=True,
    )


@dag(
    dag_id="santhosh_task1_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["santhosh", "task1", "etl"],
)
def task1_etl_pipeline():

    @task
    def check_input_files():
        client = fetch_minio_client()
        for f in [CUSTOMERS_FILE, ORDERS_FILE]:
            client.stat_object(BUCKET_NAME, f)
            print(f"Found file: {f}")
        return True

    @task
    def load_customers_staging():
        client = fetch_minio_client()
        response = client.get_object(BUCKET_NAME, CUSTOMERS_FILE)
        lines = [line.decode("utf-8") for line in response.iter_lines()]
        reader = csv.DictReader(lines)

        conn = fetch_doris_connection()
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE stg_customers;")

        insert_sql = """
            INSERT INTO stg_customers (customer_id, customer_name, city, email, created_date)
            VALUES (%s, %s, %s, %s, %s)
        """
        records = [
            (
                row["customer_id"],
                row["customer_name"],
                row["city"],
                row["email"],
                row["created_date"],
            )
            for row in reader
        ]
        cursor.executemany(insert_sql, records)
        print(f"Loaded {len(records)} records into stg_customers.")
        cursor.close()
        conn.close()

    @task
    def load_orders_staging():
        client = fetch_minio_client()
        response = client.get_object(BUCKET_NAME, ORDERS_FILE)
        lines = [line.decode("utf-8") for line in response.iter_lines()]
        reader = csv.DictReader(lines)

        conn = fetch_doris_connection()
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE stg_orders;")

        insert_sql = """
            INSERT INTO stg_orders (order_id, customer_id, order_date, quantity, unit_price)
            VALUES (%s, %s, %s, %s, %s)
        """
        records = [
            (
                row["order_id"],
                row["customer_id"],
                row["order_date"],
                row["quantity"],
                row["unit_price"],
            )
            for row in reader
        ]
        cursor.executemany(insert_sql, records)
        print(f"Loaded {len(records)} records into stg_orders.")
        cursor.close()
        conn.close()

    @task
    def transform_and_load_target():
        conn = fetch_doris_connection()
        cursor = conn.cursor()

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
        cursor.execute(transform_sql)
        print("Loaded transformed records into orders_transformed.")
        cursor.close()
        conn.close()

    @task
    def validate_load():
        conn = fetch_doris_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM orders_transformed;")
        cnt = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        if cnt == 0:
            raise ValueError("Validation failed: orders_transformed is empty!")
        print(f"Validation successful: {cnt} records in orders_transformed.")

    # Execution flow: check -> [staging] -> transform -> validate
    check = check_input_files()
    load_c = load_customers_staging()
    load_o = load_orders_staging()
    trans = transform_and_load_target()
    val = validate_load()

    check >> [load_c, load_o] >> trans >> val


task1_etl_pipeline()