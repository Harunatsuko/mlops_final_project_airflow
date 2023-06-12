import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import pandas as pd
import boto3

log = logging.getLogger(__name__)

dag = DAG(
    dag_id='update_meta_file',
    schedule_interval='* * * * *',
    start_date=datetime.now(),
    tags=['final_project'],
)

def update_meta_file(**kwargs):
    BUCKET_NAME = Variable.get('BUCKET_NAME')
    DATASET_META_FILE = Variable.get('DATASET_META_FILE')
    DATA_FOLDER = Variable.get('DATA_FOLDER')
    IMITATION_PREFIX = Variable.get('IMITATION_PREFIX')
    PHOTO_PREFIX = Variable.get('PHOTO_PREFIX')

    S3_ID = Variable.get('S3_ID')
    S3_KEY = Variable.get('SECRET_S3_KEY')

    meta_filename = os.path.join(DATA_FOLDER, DATASET_META_FILE)

    new_objs = []
    new_objs_file = os.path.join(DATA_FOLDER, 'tmp.txt')
    if os.path.exists(new_objs_file):
        with open(new_objs_file, 'r') as f:
            for obj in f:
                new_objs.append(obj.rstrip())

    flowers_imitation_objs = [obj for obj in new_objs if IMITATION_PREFIX in obj]
    flowers_photo_objs = [obj for obj in new_objs if PHOTO_PREFIX in obj]

    session = boto3.session.Session()
    s3 = session.client(service_name='s3',
                        endpoint_url='https://storage.yandexcloud.net',
                        aws_access_key_id= S3_ID,
                        aws_secret_access_key= S3_KEY)

    assert os.path.exists(meta_filename), 'meta filename was not found, pipeline crushed'
    meta = pd.read_csv(meta_filename)

    meta_imitation = pd.DataFrame({'name':flowers_imitation_objs})
    meta_imitation['label'] = IMITATION_PREFIX

    meta_photo = pd.DataFrame({'name':flowers_photo_objs})
    meta_photo['label'] = PHOTO_PREFIX

    meta_new = pd.concat([meta_imitation, meta_photo])
    meta_new['datetime'] = datetime.now()
    version = meta['version'].max()
    meta_new['version'] = version + 1
    meta = pd.concat([meta, meta_new])

    meta.to_csv(meta_filename, index==False)

    s3.upload_file(Filename=meta_filename,
                    Bucket=BUCKET_NAME,
                    Key=DATASET_META_FILE)

update_meta = PythonOperator(task_id='update_meta_file', python_callable=update_meta_file, dag=dag)

# print('Update meta file with new objects info')
# update_meta