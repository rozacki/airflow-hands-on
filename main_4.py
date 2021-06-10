import logging
import requests
import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable

def download_site_report(site_id, start_date):
    logging.info('start python operator')

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
with DAG(dag_id=f'zuhlke_python_road_data2', description='download report from one site', start_date=days_ago(1),
         default_args=args, tags=['zuhlke', 'hands-on']) as dag:

    site_id = Variable.get('site id')
    date = Variable.get('date')

    python_operator = PythonOperator(task_id='download_site_report', python_callable=download_site_report
                                     , op_kwargs={'site_id': site_id,
                                                  'start_date': datetime.datetime.fromisoformat(date)})