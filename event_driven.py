import logging
import time

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


def extract_data_func():
    logging.info(f'start extract data')
    time.sleep(10)
    logging.info('stop extract data')


def ingest_data_func():
    logging.info('start ingest data')
    time.sleep(10)
    logging.info('stop ingest data')


def validate_data_func():
    logging.info('start validate data')
    time.sleep(10)
    logging.info('stop validate data')


args = {'owner': 'zuhlke'}
with DAG(dag_id=f'event_driven', description='this is event driven dag', start_date=days_ago(10), catchup=True,
         default_args=args, tags=['zuhlke', 'hands-on', 'event-driven']) as dag:
    extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data_func,
                                  provide_context=True)

    ingest_data = PythonOperator(task_id='ingest_data', python_callable=ingest_data_func)

    validate_data = PythonOperator(task_id='validate_data', python_callable=validate_data_func)

    extract_data >> ingest_data >>  validate_data
