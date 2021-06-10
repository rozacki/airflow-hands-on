from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

'''
deploy to dags folder
show code
show graph view
show some other stats and talk through
-DAG become part of Airflow, it is as if python having UI...
delete dag - explain what happens
wait and see how it reappears - explain why
enable dag
schedule it
show logs - they are empty

'''

args = {'owner': 'zuhlke'}
with DAG(dag_id=f'zuhlke_dummy_dag', description='this is dummy dag', start_date=days_ago(1), default_args=args) as dag:
    dummy = DummyOperator(task_id='dummy_operator')

