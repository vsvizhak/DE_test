from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests
import psycopg2
import os
import logging
from dotenv import load_dotenv

load_dotenv()

OPENEXCHANGE_APP_ID = os.getenv("OPENEXCHANGE_APP_ID")

POSTGRES_1_USER= os.getenv("POSTGRES_1_USER")
POSTGRES_1_PASSWORD= os.getenv("POSTGRES_1_PASSWORD")

POSTGRES_2_USER= os.getenv("POSTGRES_2_USER")
POSTGRES_2_PASSWORD= os.getenv("POSTGRES_2_PASSWORD")


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='load_curr_data',
    default_args=default_args,
    schedule='59 * * * *',
    catchup=False,
    tags=['currency'],
    start_date=datetime(2025,8,29)
)
def load_curr_data_dag():

    @task()
    def get_rates_task():
        try:
            logging.info("Fetching latest currency rates from OpenExchangeRates API")
            url = f"https://openexchangerates.org/api/latest.json?app_id={OPENEXCHANGE_APP_ID}"
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()

            timestamp = data["timestamp"]
            rates = data["rates"]
            load_id = int(datetime.now().strftime("%Y%m%d%H%M"))

            df = pd.DataFrame(rates.items(), columns=["currency", "rate"])
            df["timestamp"] = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            df["curr_date"] = df["timestamp"].dt.strftime("%Y%m%d").astype(int)
            df["curr_time"] = df["timestamp"].dt.strftime("%H%M").astype(int)
            df["load_id"] = load_id
            df = df[["curr_date","curr_time","currency","rate","timestamp","load_id"]]

            logging.info(f"Fetched {len(df)} currency rates, load_id={load_id}")
            return df
        except Exception as e:
            logging.error(f"Error in get_rates_task: {e}")
            raise

    @task()
    def get_curr_desc_task():
        try:
            logging.info("Fetching currency descriptions from OpenExchangeRates API")
            url = f"https://openexchangerates.org/api/currencies.json?prettyprint=false&show_alternative=false&show_inactive=false&app_id={OPENEXCHANGE_APP_ID}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            df = pd.DataFrame(data.items(), columns=['curr_code', 'curr_desc'])
            logging.info(f"Fetched {len(df)} currency descriptions")
            return df
        except Exception as e:
            logging.error(f"Error in get_curr_desc_task: {e}")
            raise

    @task()
    def load_curr_stg_task(rates: pd.DataFrame):
        try:
            logging.info("Loading currency rates into CURRENCY table in Postgres-1")
            conn = psycopg2.connect(
                host="postgres-1",
                dbname="postgres",
                user=f"{POSTGRES_1_USER}",
                password=f"{POSTGRES_1_PASSWORD}"
            )
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS CURRENCY (
                      CURR_DATE       INTEGER NOT NULL
                    , CURR_TIME       INTEGER NOT NULL
                    , CURR_CODE       VARCHAR(10) NOT NULL
                    , RATE            FLOAT NOT NULL
                    , TIMESTAMP       TIMESTAMP NOT NULL
                    , LOAD_ID         BIGINT NOT NULL
                );
            """)

            cur.executemany("""
                INSERT INTO CURRENCY (CURR_DATE,CURR_TIME,CURR_CODE,RATE,TIMESTAMP,LOAD_ID)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, rates[['curr_date','curr_time','currency','rate','timestamp','load_id']].values.tolist())

            cur.execute("SELECT max(LOAD_ID) FROM CURRENCY")
            load_id = cur.fetchone()[0]
            logging.info(f"Inserted {len(rates)} rows into CURRENCY, load_id={load_id}")

            conn.commit()
            cur.close()
            conn.close()

            return load_id
        except Exception as e:
            logging.error(f"Error in load_curr_stg_task: {e}")
            raise

    @task()
    def load_curr_dwh_task(curr_desc: pd.DataFrame, load_id):
        try:
            logging.info(f"Loading currency descriptions into REF_CURRENCY in Postgres-2, using load_id={load_id}")
            conn = psycopg2.connect(
                host="postgres-2",
                dbname="postgres",
                user=f"{POSTGRES_2_USER}",
                password=f"{POSTGRES_2_PASSWORD}"
            )
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS REF_CURRENCY (
                      CURR_CODE VARCHAR(10) PRIMARY KEY
                    , CURR_DESC VARCHAR(100) NOT NULL
                );
            """)

            cur.executemany("""
                INSERT INTO REF_CURRENCY (CURR_CODE, CURR_DESC)
                VALUES (%s, %s)
                ON CONFLICT (CURR_CODE) DO UPDATE
                SET CURR_DESC = EXCLUDED.CURR_DESC
            """, curr_desc[['curr_code', 'curr_desc']].values.tolist())
            logging.info(f"Upserted {len(curr_desc)} currency descriptions into REF_CURRENCY")

            logging.info(f"Calling stored procedure SP_LOAD_DIM_CURR_PAIR({load_id})")
            cur.execute("CALL SP_LOAD_DIM_CURR_PAIR(%s)", (load_id,))
            logging.info("Stored procedure executed successfully")

            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error in load_curr_dwh_task: {e}")
            raise

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    rates = get_rates_task()
    currency = get_curr_desc_task()
    load_id = load_curr_stg_task(rates)
    dwh = load_curr_dwh_task(currency, load_id)

    start >> rates >> currency >> load_id >> dwh >> end

dag = load_curr_data_dag()
