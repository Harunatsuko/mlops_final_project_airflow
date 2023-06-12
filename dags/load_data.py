import os
import logging
import jwt
from datetime import datetime
import requests
import json
import time

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import pandas as pd
import boto3

from instance_utils import get_vm_ip_address

log = logging.getLogger(__name__)

# dag = DAG(
#     dag_id='load_data',
#     schedule_interval='* * * * *',
#     start_date=datetime.now(),
#     tags=['final_project'],
# )


def load_data_on_server(**kwargs):
    DATA_FOLDER = Variable.get('DATA_FOLDER')
    new_objs = []
    new_objs_file = os.path.join(DATA_FOLDER, 'tmp.txt')
    if os.path.exists(new_objs_file):
        with open(new_objs_file, 'r') as f:
            for obj in f:
                new_objs.append(obj.rstrip())
    ip_addr = get_vm_ip_address()

    url = 'http://{}:8002/load_data'.format(ip_addr)
    res = requests.post(url, json={'objects_list': new_objs})

# print('Load new data')
# load_data