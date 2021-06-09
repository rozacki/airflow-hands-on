import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

'''
deploy to dags folder
show code
show graph view
enable dag
schedule it
show logs - they are not empty

'''

def python_callable():
    logging.info('logging from python operator')


args = {'owner': 'chris'}
with DAG(dag_id=f'python_dag', description='this is dag with single python operator', start_date=days_ago(1),
         default_args=args) as dag:
    python_operator = PythonOperator(task_id='python_operator', python_callable=python_callable)