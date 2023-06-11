import logging
from datetime import datetime
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import pandas as pd
import boto3

log = logging.getLogger(__name__)

dag = DAG(
    dag_id='train_model',
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

def train_model(**kwargs):
    ip_addr = get_vm_ip_address()

    # 2 epochs for test purposes
    num_epochs = 2

    url = 'http://{}:8002/train_model/{}'.format(ip_addr, num_epochs)
    res = requests.get(url)

train = PythonOperator(task_id='train_model',
                            python_callable=train_model,
                            dag=dag)

print('Train model')
train