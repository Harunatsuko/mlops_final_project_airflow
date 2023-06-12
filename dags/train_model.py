import logging
from datetime import datetime
import requests
import json
import time
import jwt

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import pandas as pd
import boto3

from instance_utils import get_vm_ip_address

log = logging.getLogger(__name__)

# dag = DAG(
#     dag_id='train_model',
#     schedule_interval='* * * * *',
#     start_date=datetime.now(),
#     tags=['final_project'],
# )

def train_model(**kwargs):
    ip_addr = get_vm_ip_address()

    # 2 epochs for test purposes
    num_epochs = 2

    url = 'http://{}:8002/train_model/{}'.format(ip_addr, num_epochs)
    res = requests.get(url)

# print('Train model')
# train