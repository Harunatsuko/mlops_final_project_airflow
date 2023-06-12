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

log = logging.getLogger(__name__)

# dag = DAG(
#     dag_id='train_model',
#     schedule_interval='* * * * *',
#     start_date=datetime.now(),
#     tags=['final_project'],
# )

iam_token_url = 'https://iam.api.cloud.yandex.net/iam/v1/tokens'

def gen_token():
    service_account_id = Variable.get('SERV_ACC_ID')
    key_id = Variable.get('SERV_ACC_PUB_KEY_ID')
    secret_filepath = Variable.get('SECRET_FILEPATH')

    with open(secret_filepath, 'r') as private:
        private_key = json.load(private) # Чтение закрытого ключа из файла.
        private_key = private_key['private_key']

    now = int(time.time())
    payload = {
            'aud': 'https://iam.api.cloud.yandex.net/iam/v1/tokens',
            'iss': service_account_id,
            'iat': now,
            'exp': now + 360}

    encoded_token = jwt.encode(
        payload,
        private_key,
        algorithm='PS256',
        headers={'kid': key_id})

    return encoded_token

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

def train_model(**kwargs):
    ip_addr = get_vm_ip_address()

    # 2 epochs for test purposes
    num_epochs = 2

    url = 'http://{}:8002/train_model/{}'.format(ip_addr, num_epochs)
    res = requests.get(url)

# print('Train model')
# train