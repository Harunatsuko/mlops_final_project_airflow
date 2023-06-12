import logging
from datetime import datetime
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import pandas as pd
import boto3

from instance_utils import get_vm_ip_address

log = logging.getLogger(__name__)

# dag = DAG(
#     dag_id='load_weights',
#     schedule_interval='* * * * *',
#     start_date=datetime.now(),
#     tags=['final_project'],
# )

def load_weights(**kwargs):
    ip_addr = get_vm_ip_address()

    url = 'http://{}:8002/save_model/'.format(ip_addr)
    res = requests.get(url)
    if res.status_code == 200:
        return res.json()['is_new']
    return False

# print('Upload new weights to s3')
# upload_weights