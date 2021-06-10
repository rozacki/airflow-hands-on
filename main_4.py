import logging
import requests
import datetime
import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import pandas as pd
import json
import numpy as np


def get_site_daily_traffic_stats(json_data):
    df = pd.DataFrame(json_data['Rows'])
    df['Total Traffic Volume'] = df['Total Volume'].astype(int)
    df['Avg mph'] = df['Avg mph'].astype(int)
    df['Total mph'] = df['Avg mph'] * df['Total Traffic Volume']
    grouped_df = pd.DataFrame(df.groupby(['Site Name', 'Report Date']).agg(
        {
            'Total Traffic Volume': 'sum',
            'Total mph': 'sum'
        })
    ).reset_index()

    grouped_df['Avg mph'] = np.round(grouped_df['Total mph'] / grouped_df['Total Traffic Volume'], 2)
    return grouped_df.drop('Total mph', axis=1).to_dict('records')


def download_site_report_proc(site_id, start_date):
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
        logging.debug(f'highway england data {res.json()}')
    else:
        logging.debug(f'highway england data error code {res.status_code}')


def process_site_report_proc():
    cwd = os.path.dirname(os.path.realpath(__file__))
    data_file = os.path.join(cwd, f'data_file.json')
    logging.info(f'data stored in {data_file}')

    with open(data_file, 'r') as f:
        json_data = json.loads(f.readlines()[0])

    stats = get_site_daily_traffic_stats(json_data)

    logging.info(f'site statistic {stats}')

args = {'owner': 'zuhlke'}
with DAG(dag_id=f'zuhlke_python_road_data2', description='download report from one site', start_date=days_ago(1),
         default_args=args, tags=['zuhlke', 'hands-on']) as dag:

    site_id = Variable.get('site id')
    date = Variable.get('date')

    download_site_report = PythonOperator(task_id='download_site_report', python_callable=download_site_report_proc
                                          , op_kwargs={'site_id': site_id,
                                                  'start_date': datetime.datetime.fromisoformat(date)})

    process_site_report = PythonOperator(task_id='process_site_report', python_callable=process_site_report_proc)

    download_site_report >> process_site_report
