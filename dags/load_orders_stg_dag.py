from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import uuid
import random
import faker
import psycopg2
import logging

fake = faker.Faker()
logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='load_orders_stg',
    default_args=default_args,
    schedule='*/10 * * * *',
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=['orders'],
)
def load_orders_stg_dag():

    @task()
    def load_orders_stg_task():
        try:
            logger.info("Connecting to Postgres-2 to fetch currencies...")
            conn_source = psycopg2.connect(
                host="postgres-2",
                dbname="postgres",
                user="postgres",
                password="postgres"
            )
            cur_source = conn_source.cursor()
            cur_source.execute("SELECT CURR_CODE FROM REF_CURRENCY")
            rows = cur_source.fetchall()
            currencies = [row[0] for row in rows]
            logger.info(f"Fetched {len(currencies)} currencies from REF_CURRENCY.")
            cur_source.close()
            conn_source.close()

            if not currencies:
                logger.warning("No currencies found in REF_CURRENCY!")
                return

            logger.info("Connecting to Postgres-1 to insert orders...")
            conn_target = psycopg2.connect(
                host="postgres-1",
                dbname="postgres",
                user="postgres",
                password="postgres"
            )
            cur_target = conn_target.cursor()

            logger.info("Creating ORDERS table if not exists...")
            cur_target.execute("""
                CREATE TABLE IF NOT EXISTS ORDERS (
                      ORDER_ID          UUID PRIMARY KEY
                    , CUSTOMER_EMAIL    VARCHAR(100)
                    , ORDER_DATE        TIMESTAMP
                    , AMOUNT            NUMERIC(10,2)
                    , CURRENCY          VARCHAR(10)
                );
            """)

            logger.info("Generating 5000 fake orders...")
            orders = []
            for _ in range(5000):
                order_id = str(uuid.uuid4())
                customer_email = fake.email()
                order_date = fake.date_time_between(start_date="-7d", end_date="now")
                amount = round(random.uniform(10, 500), 2)
                currency = random.choice(currencies)
                orders.append((order_id, customer_email, order_date, amount, currency))
            logger.info("Orders generated.")

            logger.info("Inserting orders into ORDERS table...")
            cur_target.executemany("""
                INSERT INTO ORDERS (ORDER_ID, CUSTOMER_EMAIL, ORDER_DATE, AMOUNT, CURRENCY)
                VALUES (%s, %s, %s, %s, %s)
            """, orders)
            conn_target.commit()
            logger.info("Orders inserted successfully.")

        except Exception as e:
            logger.error(f"Error in load_orders_stg_task: {e}")
            if 'conn_target' in locals():
                conn_target.rollback()
            raise

        finally:
            if 'cur_target' in locals():
                cur_target.close()
            if 'conn_target' in locals():
                conn_target.close()
            logger.info("Connections closed.")

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    load_orders_stg = load_orders_stg_task()
    start >> load_orders_stg >> end

dag = load_orders_stg_dag()
