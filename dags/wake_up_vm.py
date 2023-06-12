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

from instance_utils import gen_token

log = logging.getLogger(__name__)

# dag = DAG(
#     dag_id='wake_up_vm',
#     schedule_interval='* * * * *',
#     start_date=datetime.now(),
#     tags=['final_project'],
# )

INSTANCE_ID = Variable.get('INSTANCE_ID')
start_url = 'https://compute.api.cloud.yandex.net/compute/v1/instances/{}:start'.format(INSTANCE_ID)

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