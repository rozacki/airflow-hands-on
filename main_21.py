import logging
import time
import random

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


def start_proc(log_this='Let''s start branching'):
    logging.info(log_this)


def parallel_one_proc():
    logging.info('EXECUTE ONE')


def parallel_two_proc(sleep_for=10, choices=['fail', 'pass']):
    logging.info('EXECUTE TWO')
    time.sleep(sleep_for)
    choice = random.choices(choices)
    logging.info(choice)
    if choice == ['fail']:
        raise Exception('Chose to fail')


def join_parallel_proc():
    logging.info('JOINED BOTH')


args = {'owner': 'zuhlke'}
with DAG(dag_id=f'zuhlke_branching_dag', description='this is dag with single python operator', start_date=days_ago(1),
         default_args=args, tags=['zuhlke', 'hands-on']) as dag:
    start_here = PythonOperator(task_id='start_here', python_callable=start_proc)

    parallel_one = PythonOperator(task_id='parallel_one', python_callable=parallel_one_proc)

    parallel_two = PythonOperator(task_id='parallel_two', python_callable=parallel_two_proc)

    join_parallel = PythonOperator(task_id='join_parallel', python_callable=join_parallel_proc)

    start_here >> [parallel_one, parallel_two] >> join_parallel


# args = {'owner': 'zuhlke'}
# with DAG(dag_id=f'zuhlke_branching_dag', description='this is dag with single python operator', start_date=days_ago(1),
#          default_args=args) as dag:
#     start_here = PythonOperator(task_id='start_here', python_callable=start_proc)
#
#     parallel_one = PythonOperator(task_id='parallel_one', python_callable=parallel_one_proc)
#
#     parallel_two = PythonOperator(task_id='parallel_two', python_callable=parallel_two_proc)
#
#     # all_success | all_failed | all_done | one_success | one_failed | none_failed | none_failed_or_skipped
#     # | none_skipped | dummy
#     # default is all_success
#     join_parallel = PythonOperator(task_id='join_parallel', python_callable=join_parallel_proc,
#                                    trigger_rule=trigger_rule.TriggerRule.ALL_SUCCESS)
#
#     start_here >> [parallel_one, parallel_two] >> join_parallel