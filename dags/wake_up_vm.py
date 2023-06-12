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

log = logging.getLogger(__name__)

# dag = DAG(
#     dag_id='wake_up_vm',
#     schedule_interval='* * * * *',
#     start_date=datetime.now(),
#     tags=['final_project'],
# )

INSTANCE_ID = Variable.get('INSTANCE_ID')
start_url = 'https://compute.api.cloud.yandex.net/compute/v1/instances/{}:start'.format(INSTANCE_ID)
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


def wake_up_vm():
    jwt_token = gen_token()
    IAM_TOKEN = requests.post(iam_token_url,
                            json={"jwt":jwt_token.decode('ascii')},
                            headers = {'Content-Type':'application/json'})
    iam_token = IAM_TOKEN.json()['iamToken']
    wake_up = requests.post(start_url,
                            headers = {'Authorization': 'Bearer {}'.format(iam_token)})
    log.info(wake_up.text)

# print('Wake up vm with gpu')
# wake_up