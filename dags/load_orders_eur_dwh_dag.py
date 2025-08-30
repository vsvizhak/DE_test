from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import psycopg2
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DWH = {
    "host": "postgres-2",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres"
}

@dag(
    dag_id='load_orders_eur_dwh',
    default_args=default_args,
    schedule='@hourly',
    catchup=False,
    tags=['orders_eur']
)
def load_orders_eur_dwh():

    @task()
    def load_orders_eur_task():
        try:
            logging.info("Connecting to DWH (Postgres-2)")
            conn = psycopg2.connect(**DWH)
            cur = conn.cursor()

            logging.info("Calling stored procedure SP_LOAD_ORDERS_EUR()")
            cur.execute("CALL SP_LOAD_ORDERS_EUR()")
            logging.info("Stored procedure executed successfully")

            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error in load_orders_eur_task: {e}")
            raise

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    load_orders_eur = load_orders_eur_task()

    start >> load_orders_eur >> end

dag = load_orders_eur_dwh()
