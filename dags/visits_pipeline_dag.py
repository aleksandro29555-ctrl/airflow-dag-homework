from datetime import datetime
import csv
import logging

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


POSTGRES_CONN_ID = "postgres_default"
CSV_PATH = "/opt/airflow/data/new_visits.csv"


def load_csv_to_visits():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    insert_sql = """
        INSERT INTO bd_shops.visits (id, product_id, visit_date, line_size, employer_id, shop_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """

    conn = hook.get_conn()
    cursor = conn.cursor()

    with open(CSV_PATH, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        rows = [
            (
                int(row["id"]),
                int(row["product_id"]),
                row["visit_date"],
                float(row["line_size"]),
                int(row["employer_id"]),
                int(row["shop_id"]),
            )
            for row in reader
        ]

    cursor.executemany(insert_sql, rows)
    conn.commit()

    cursor.close()
    conn.close()


def refresh_mart():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run("SELECT bd_shops.refresh_daily_shop_visits_mart();")


def log_success():
    logging.info("DAG completed successfully: CSV loaded and mart refreshed.")


with DAG(
    dag_id="visits_pipeline_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["homework", "shops"],
) as dag:

    load_csv_task = PythonOperator(
        task_id="load_csv_to_visits",
        python_callable=load_csv_to_visits,
    )

    refresh_mart_task = PythonOperator(
        task_id="refresh_daily_shop_visits_mart",
        python_callable=refresh_mart,
    )

    log_success_task = PythonOperator(
        task_id="log_success",
        python_callable=log_success,
    )

    load_csv_task >> refresh_mart_task >> log_success_task