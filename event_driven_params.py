import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


def extract_data_func(**kwargs):
    logging.info('dump kwargs')
    snapshot_id = kwargs['dag_run'].conf['snapshot_id']
    logging.info(f'start extract data for snapshot_id {snapshot_id}')
    logging.info('stop extract data')
    ti = kwargs['ti']
    ti.xcom_push(key='file_name', value=f's3://source-system/data/{snapshot_id}.json')


def ingest_data_func(**kwargs):
    ti = kwargs['ti']
    file_name = ti.xcom_pull(key='file_name')
    logging.info(f'start ingest data for {file_name}')
    logging.info('stop ingest data')
    ti.xcom_push(key='primary_key', value=f'pk_file_name_{datetime.now()}')


def validate_data_func(**kwargs):
    ti = kwargs['ti']
    primary_key = ti.xcom_pull(key='primary_key')
    logging.info(f'start validating data {primary_key}')
    logging.info('stop validating data')
    #raise Exception('fail...')


default_args = {'owner': 'zuhlke'}
with DAG(dag_id=f'event_driven_params', description='this is event driven dag by parameters', start_date=days_ago(10),
         catchup=True,
         default_args=default_args, tags=['zuhlke', 'hands-on', 'event-driven']) as dag:
    extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data_func, retries=3,
                                  retry_delay=timedelta(seconds=1))

    ingest_data = PythonOperator(task_id='ingest_data', python_callable=ingest_data_func)

    validate_data = PythonOperator(task_id='validate_data', python_callable=validate_data_func)

    extract_data >> ingest_data >> validate_data
