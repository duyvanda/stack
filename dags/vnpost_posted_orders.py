from utils.df_handle import *
from requests.structures import CaseInsensitiveDict
import requests
import json

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='POSTED_ORDERS'
prefix='VNPOST'
csv_path = f'/usr/local/airflow/plugins/{prefix}{name}/'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2022, 5, 14, tzinfo=local_tz),
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
          schedule_interval= '@daily',
          tags=[prefix+name, '@daily']
)

def insert():
    datenow_1day_ago = ( datetime.now()-timedelta(1) ).strftime("%Y-%m-%d")
    datenow_2day_ago = ( datetime.now()-timedelta(2) ).strftime("%Y-%m-%d")
    datenow = datetime.now().strftime("%Y-%m-%d")
    print(datenow_1day_ago)
    print(datenow_2day_ago)
    print(datenow)
    # datenow_2day_ago
    # datenow
    date_lst = []
    date_lst.append(datenow_2day_ago)
    date_lst.append(datenow_1day_ago)
    # date_lst
    for i in date_lst:
        print(i)
        y = int(i.split("-")[0])
        m = int(i.split("-")[1])
        d = int(i.split("-")[2])
        data = {
        "year":y,
        "month":m,
        "day":d
        }
        url = """https://birest-6ey4kecoka-as.a.run.app/api/getvnporders/"""
        resp = requests.post(url, json=data)
        # resp.json()
        df = pd.DataFrame(resp.json())
        if df.shape[0] > 0:
            col_list = [
            'ReceiverWardId',
            'ReceiverFullname',
            'SERVER_TIMESTAMP',
            'CustomerNote',
            'SenderDistrictId',
            'OrderCode',
            'SenderProvinceId',
            'PickupType',
            'ItemCode',
            'CustomerCode',
            'ReceiverDistrictId',
            'ReceiverAddress',
            'CustomerId',
            'ReceiverTel',
            'SenderTel',
            'ServiceName',
            'SenderWardId',
            'SenderFullname',
            'ReceiverProvinceId',
            'Id',
            'SenderAddress'
            ]
            df.SERVER_TIMESTAMP = pd.to_datetime(df.SERVER_TIMESTAMP)
            df.CreateTime = pd.to_datetime(df.CreateTime)
            df.DeliveryDateEvaluation = pd.to_datetime(df.DeliveryDateEvaluation)
            execute_bq_query(f"""DELETE FROM `spatial-vision-343005.biteam.d_vnpost_postedorders` WHERE DATE(server_timestamp) = "{i}" """)
            bq_values_insert(df[col_list], "d_vnpost_postedorders", 2)
            print("done")
        else:
            print("nodata")
            pass


dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

dummy_start >> insert