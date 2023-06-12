import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.branch import BaseBranchOperator

from load_weights import load_weights
from update_meta_file import update_meta_file

log = logging.getLogger(__name__)

class WeightsBranchOperator(BaseBranchOperator):
    def choose_branch(self, context):
        is_new = load_weights()
        if is_new:
            return 'update_meta_file'
        else:
            return None

dag = DAG(
    dag_id='final_project_dag_step2',
    schedule_interval=timedelta(weeks=1),
    start_date=datetime(2023, 6, 14, 2, 5, 0),
    tags=['final_project'],
)

upload_weights = WeightsBranchOperator(task_id='load_weights',
                                        dag=dag)

update_meta = PythonOperator(task_id='update_meta_file',
                             python_callable=update_meta_file,
                             dag=dag)

upload_weights >> update_meta