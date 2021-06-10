from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator


args = {'owner': 'zuhlke'}
with DAG(dag_id=f'zuhlke_dummy_dag', description='this is dummy dag', start_date=days_ago(1), default_args=args,
         tags=['zuhlke', 'hands-on']) as dag:
    dummy = DummyOperator(task_id='dummy_operator')

