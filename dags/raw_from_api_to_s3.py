import logging

import duckdb
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

# Конфигурация ДАГ
OWNER = 'k.polshkov'
DAG_ID = 'raw_from_api_to_s3'

# Используемые таблицы в ДАГ
LAYER = 'raw'
SOURCE = 'earthquake'

# Minio keys
ACCESS_KEY = Variable.get('access_key')
SECRET_KEY = Variable.get('secret_key')

args = {
    'owner': OWNER,
    'start_date': pendulum.datetime(year=2025, month=9, day=3, tz='Europe/Moscow'),
    'catchup': True,
    'retries': 5,
    'retry_delay': pendulum.duration(seconds=10),
}

def get_dates(**context) -> tuple[str, str]:

    start_date = context['data_interval_start'].format('YYYY-MM-DD')
    end_date = context['data_interval_end'].format('YYYY-MM-DD')
    
    return start_date, end_date

def get_and_transfer_data_to_s3_minio(**context):

    start_date, end_date = get_dates(**context)
    logging.info(f'Start load for dates:{start_date}/{end_date}')
    con = duckdb.connect()

    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key_id = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        COPY
        (
        SELECT *
        FROM read_csv_auto('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}') as res
        )
        TO 's3://project1/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """,
    )

    con.close()
    logging.info(f'Download for date success:{start_date}')

with DAG(
    dag_id=DAG_ID,
    schedule= '0 5 * * *',
    default_args=args,
    tags=['minio_s3','raw'],
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(
        task_id ='start',
    )

    get_and_transfer_data_to_s3_minio = PythonOperator(
        task_id='get_and_transfer_data_to_s3_minio',
        python_callable=get_and_transfer_data_to_s3_minio,
    )

    end = EmptyOperator(
        task_id = 'end',
    )

    start >> get_and_transfer_data_to_s3_minio >> end