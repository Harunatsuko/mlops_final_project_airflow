import logging
import shutil
import time
from datetime import datetime
from pprint import pprint

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import boto3

log = logging.getLogger(__name__)

dag = DAG(
    dag_id='load_weights',
    schedule_interval='* * * * *',
    start_date=datetime.now(),
    tags=['final_project'],
)

def get_vm_ip_address():
    INSTANCE_ID = Variable.get('INSTANCE_ID')
    url = 'https://compute.api.cloud.yandex.net/compute/v1/instances/{}'.format(INSTANCE_ID)
    res = requests.get(url)
    print(res)
    ip_addr = res['networkInterfaces']['primaryV4Address']['address']
    return ip_addr

def load_weights(**kwargs):
    ip_addr = get_vm_ip_address()

    url = 'http://{}:8002/save_model/'.format(ip_addr)
    res = requests.get(url)

upload_weights = PythonOperator(task_id='load_weights',
                                python_callable=load_weights,
                                dag=dag)

print('Upload new weights to s3')
upload_weights