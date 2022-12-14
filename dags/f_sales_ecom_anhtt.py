# -*- coding: utf-8 -*-
"""f_sales_ecom_anhtt.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1y3Wr1GO0PKOlZyyxuFSd0NcCiVsI8ssm
"""

from utils.df_handle import *

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import requests
from requests.structures import CaseInsensitiveDict
# from openpyxl import Workbook, load_workbook
from datetime import datetime  
from datetime import timedelta 

local_tz = pendulum.timezone("Asia/Bangkok")
name='SalesEcom'
prefix='Anhtt_'
path = f'/usr/local/airflow/plugins/{prefix}{name}/'

# datenow_min1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

dag_params = {
    'owner': 'duyvan',
    "depends_on_past": False,
    'start_date': datetime(2022, 7, 15, tzinfo=local_tz),
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    # 'email':['duyvq@merapgroup.com', 'vanquangduy10@gmail.com'],
    'do_xcom_push': False,
    'execution_timeout':timedelta(seconds=300)
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=10),
}

dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          schedule_interval= '0 10,12,14,16,17,20 * * *',
          tags=[prefix+name, 'Daily', 'at17']
)

# from utils.df_handle import *
def update():
    df=pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vQgGhbZQhIW_JmDnfp352nGDMhNpetFCbB_qU_Q-0BLuNv-fRxoDjHQDJU6u2r5y3nynlhDU070Bs4E/pub?gid=0&single=true&output=csv")
    df.columns = lower_col(df)
    df.columns = cleancols(df)
    df.columns
    df = df[['madonhang', 'mdsmdcrs']]
    df['manv'] = df.mdsmdcrs.str.strip().str[0:6]
    drop_cols(df, 'mdsmdcrs')
    df['inserted_at'] = datetime.now()
    bq_values_insert(df, "f_sales_ecom_anhtt", 3)

# Dont Execute this
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

update = PythonOperator(task_id="update", python_callable=update, dag=dag)


dummy_start >> update