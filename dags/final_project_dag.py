import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from check_new_data import check_new_objs
from wake_up_vm import wake_up
from load_data import load_data
from train_model import train

log = logging.getLogger(__name__)

dag = DAG(
    dag_id='final_project_dag_step1',
    schedule_interval='* * * * *',
    start_date=datetime.now(),
    tags=['final_project'],
)

@task.branch(task_id='branch_task')
def branch_func():
    DATA_FOLDER = Variable.get('DATA_FOLDER')
    tmp_filepath = os.path.join(DATA_FOLDER, 'tmp.txt')
    if os.path.exists(tmp_filepath):
        return 'wake_up_vm'
    else:
        return None

branch_op = branch_func()

check_new_objs >> branch_op >> wake_up_vm >> load_data >> train_model