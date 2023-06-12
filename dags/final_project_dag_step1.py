import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.branch import BaseBranchOperator

from check_new_data import check_new_data
from wake_up_vm import wake_up_vm
from load_data import load_data_on_server
from train_model import train_model

log = logging.getLogger(__name__)

class MyBranchOperator(BaseBranchOperator):
    def choose_branch(self, context):
        DATA_FOLDER = Variable.get('DATA_FOLDER')
        tmp_filepath = os.path.join(DATA_FOLDER, 'tmp.txt')
        if os.path.exists(tmp_filepath):
            return 'wake_up_vm'
        else:
            return None

dag = DAG(
    dag_id='final_project_dag_step1',
    schedule_interval='* * * * *',
    start_date=datetime.now(),
    tags=['final_project'],
)

branch_op = MyBranchOperator(task_id='branch',
                            dag=dag)

wake_up = PythonOperator(task_id='wake_up_vm',
                        python_callable=wake_up_vm,
                        dag=dag)

check_new_objs = PythonOperator(task_id='check_new_data',
                                python_callable=check_new_data,
                                dag=dag)

load_data = PythonOperator(task_id='load_data_on_server',
                            python_callable=load_data_on_server,
                            dag=dag)

train = PythonOperator(task_id='train_model',
                            python_callable=train_model,
                            dag=dag)

check_new_objs >> branch_op >> wake_up_vm >> load_data >> train_model