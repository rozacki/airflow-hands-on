import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


def python_callable():
    logging.info('logging from python operator')


args = {'owner': 'zuhlke'}
with DAG(dag_id=f'zuhlke_python_dag', description='this is dag with single python operator', start_date=days_ago(1),
         default_args=args, tags=['zuhlke', 'hands-on']) as dag:
    python_operator = PythonOperator(task_id='python_operator', python_callable=python_callable)