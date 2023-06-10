import logging
import shutil
import time
from datetime import datetime
from pprint import pprint

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator

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
    IMITATION_PREFIX = Variable.get('IMITATION_PREFIX')
    PHOTO_PREFIX = Variable.get('PHOTO_PREFIX')

    ti = kwargs['ti']
    new_objs = ti.xcom_pull(task_ids='check_new_data')
    print(new_objs)
    flowers_imitation_objs = [obj for obj in new_objs if IMITATION_PREFIX in obj]
    flowers_photo_objs = [obj for obj in new_objs if PHOTO_PREFIX in obj]

    session = boto3.session.Session()
    s3 = session.client(service_name='s3',
                        endpoint_url='https://storage.yandexcloud.net')

    contents = s3.list_objects(Bucket=BUCKET_NAME)
    if 'Contents' in contents.keys():
        for key in contents['Contents']:
            if key['Key'] == DATASET_META_FILE:
                s3.download_file(BUCKET_NAME, DATASET_META_FILE, DATASET_META_FILE)
                meta = pd.read_csv(DATASET_META_FILE)

    meta_imitation = pd.DataFrame({'name':flowers_imitation_objs})
    meta_imitation['label'] = IMITATION_PREFIX

    meta_photo = pd.DataFrame({'name':flowers_photo_objs})
    meta_photo['label'] = PHOTO_PREFIX

    meta_new = pd.concat([meta_imitation, meta_photo])
    meta_new['datetime'] = datetime.now()
    version = meta['version'].max()
    meta_new['version'] = version + 1
    meta = pd.concat([meta, meta_new])

    meta.to_csv(DATASET_META_FILE, index==False)

    s3.upload_file(Filename=DATASET_META_FILE,
                    Bucket=BUCKET_NAME,
                    Key=DATASET_META_FILE)

update_meta = PythonOperator(task_id='update_meta_file', python_callable=update_meta_file, dag=dag)

print('Update meta file with new objects info')
update_meta