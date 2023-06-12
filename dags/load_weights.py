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

# dag = DAG(
#     dag_id='load_weights',
#     schedule_interval='* * * * *',
#     start_date=datetime.now(),
#     tags=['final_project'],
# )

def get_vm_ip_address():
    INSTANCE_ID = Variable.get('INSTANCE_ID')

    jwt_token = gen_token()
    IAM_TOKEN = requests.post(iam_token_url,
                            json={"jwt":jwt_token.decode('ascii')},
                            headers = {'Content-Type':'application/json'})
    iam_token = IAM_TOKEN.json()['iamToken']

    url = 'https://compute.api.cloud.yandex.net/compute/v1/instances/{}'.format(INSTANCE_ID)
    res = requests.get(url, headers = {'Authorization': 'Bearer {}'.format(iam_token)})
    print(res)
    ip_addr = res.json()['networkInterfaces'][0]['primaryV4Address']['address']
    return ip_addr

def load_weights(**kwargs):
    ip_addr = get_vm_ip_address()

    url = 'http://{}:8002/save_model/'.format(ip_addr)
    res = requests.get(url)
    if res.status_code == 200:
        return res.json()['is_new']
    return False

# print('Upload new weights to s3')
# upload_weights