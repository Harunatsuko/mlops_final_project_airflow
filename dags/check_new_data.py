import os
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
    dag_id='check_new_data',
    schedule_interval='* * * * *',
    start_date=datetime.now(),
    tags=['final_project'],
)

def create_meta_file(s3, flowers_imitation_objs, flowers_photo_objs):
    BUCKET_NAME = Variable.get('BUCKET_NAME')
    DATASET_META_FILE = Variable.get('DATASET_META_FILE')
    DATA_FOLDER = Variable.get('DATA_FOLDER')
    IMITATION_PREFIX = Variable.get('IMITATION_PREFIX')
    PHOTO_PREFIX = Variable.get('PHOTO_PREFIX')

    meta_imitation = pd.DataFrame({'name':flowers_imitation_objs})
    meta_imitation['label'] = IMITATION_PREFIX

    meta_photo = pd.DataFrame({'name':flowers_photo_objs})
    meta_photo['label'] = PHOTO_PREFIX

    meta = pd.concat([meta_imitation, meta_photo])
    meta['datetime'] = datetime.now()
    meta['version'] = 0

    meta_filename = os.path.join(DATA_FOLDER, DATASET_META_FILE)
    meta.to_csv(meta_filename, index=False)

    s3.upload_file(Filename=meta_filename,
                    Bucket=BUCKET_NAME,
                    Key=DATASET_META_FILE)

def new_obj_list(meta, flowers_imitation_objs, flowers_photo_objs):
    IMITATION_PREFIX = Variable.get('IMITATION_PREFIX')
    PHOTO_PREFIX = Variable.get('PHOTO_PREFIX')

    new_objs = []
    flowers_imitation_objs_meta = meta[meta['label'] == IMITATION_PREFIX]['name'].values
    flowers_photo_objs_meta = meta[meta['label'] == PHOTO_PREFIX]['name'].values

    flowers_imitation_new = list(set(flowers_imitation_objs).difference(flowers_imitation_objs_meta))
    flowers_photo_new = list(set(flowers_photo_objs).difference(flowers_photo_objs_meta))
    if len(flowers_imitation_new) or len(flowers_photo_new):
        new_objs = flowers_imitation_new + flowers_photo_new
    log.info('Len of new_objs {}'.format(len(new_objs)))
    return new_objs

def check_new_data():
    BUCKET_NAME = Variable.get('BUCKET_NAME')
    DATASET_META_FILE = Variable.get('DATASET_META_FILE')
    DATA_FOLDER = Variable.get('DATA_FOLDER')
    IMITATION_PREFIX = Variable.get('IMITATION_PREFIX')
    PHOTO_PREFIX = Variable.get('PHOTO_PREFIX')

    S3_ID = Variable.get('S3_ID')
    S3_KEY = Variable.get('SECRET_S3_KEY')

    session = boto3.session.Session()
    s3 = session.client(service_name='s3',
                        endpoint_url='https://storage.yandexcloud.net',
                        aws_access_key_id= S3_ID,
                        aws_secret_access_key= S3_KEY)

    flowers_imitation_objs = []
    flowers_photo_objs = []
    meta = None

    contents = s3.list_objects(Bucket=BUCKET_NAME)
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_NAME)
    for page in pages:
        for key in page['Contents']:
            if key['Key'].split('/')[0] == IMITATION_PREFIX:
                flowers_imitation_objs.append(key['Key'])
            elif key['Key'].split('/')[0] == PHOTO_PREFIX:
                flowers_photo_objs.append(key['Key'])
            else:
                if key['Key'] == DATASET_META_FILE:
                    meta_filename = os.path.join(DATA_FOLDER, DATASET_META_FILE)
                    s3.download_file(BUCKET_NAME, DATASET_META_FILE, meta_filename)
                    meta = pd.read_csv(meta_filename)
    if meta is not None:
        return new_obj_list(meta, flowers_imitation_objs, flowers_photo_objs)
    else:
        create_meta_file(s3, flowers_imitation_objs, flowers_photo_objs)
        return flowers_imitation_objs+flowers_photo_objs

check_new_objs = PythonOperator(task_id='check_new_data', python_callable=check_new_data, dag=dag)

print('Check new data')
check_new_objs