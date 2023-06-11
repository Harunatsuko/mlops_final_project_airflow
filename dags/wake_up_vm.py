import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

log = logging.getLogger(__name__)

INSTANCE_ID = Variable.get('INSTANCE_ID')
url = 'https://compute.api.cloud.yandex.net/compute/v1/instances/{}:start'.format(INSTANCE_ID)
cmd1 = 'export IAM_TOKEN=`yc iam create-token`'
cmd2 = 'curl -X POST -H "Authorization: Bearer ${IAM_TOKEN}" '
cmd3 = 'export IAM_TOKEN=""'

dag = DAG(
    dag_id='wake_up_vm',
    schedule_interval='* * * * *',
    start_date=datetime.now(),
    tags=['final_project'],
)

wake_up_vm = BashOperator(
    task_id='wake_up_vm',
    bash_command=cmd1 + ' && ' + cmd2 + url + ' && ' + cmd3,
    dag=dag,
    run_as_user='finalproject'
)

print('Wake up vm with gpu')
wake_up_vm