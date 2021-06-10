import logging
import requests
import datetime

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

def download_site_report(site_id=100, start_date=datetime.datetime.fromisoformat('2021-04-01')):
    logging.info('start python operator')
    import json

    enddate = start_date + datetime.timedelta(days=1)

    endpoint = 'http://webtris.highwaysengland.co.uk/api/v1.0/reports/daily?sites={site_id}' \
               '&start_date={start_day:02}{start_month:02}{start_year}' \
               '&end_date={stop_day:02}{stop_month:02}{stop_year}&page=1&page_size=96'

    url = endpoint.format(site_id=site_id, start_year=start_date.year, start_month=start_date.month,
                    start_day=start_date.day, stop_year=enddate.year, stop_month=enddate.month, stop_day=enddate.day)

    logging.info(url)
    res = requests.get(url)
    if res.status_code == 200:
        logging.debug(f'HE data {res.json()}')
    else:
        logging.debug(f'HE data error code {res.status_code}')


args = {'owner': 'zuhlke'}
with DAG(dag_id=f'zuhlke_python_road_data', description='download report from one site', start_date=days_ago(1),
         default_args=args) as dag:
    python_operator = PythonOperator(task_id='download_site_report', python_callable=download_site_report
                                     , op_kwargs={'site_id': 1000,
                                               'start_date': datetime.datetime.fromisoformat('2021-04-01')})