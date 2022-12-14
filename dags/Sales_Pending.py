# -*- coding: utf-8 -*-
"""f_sales_pend.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1Gv4TJMgxnLP4CFit3NFDFF8kjnBH9Ci6
"""

# DON'T USE THIS CELL
from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")
name='Pending'
prefix='Sales_'
path = f'/usr/local/airflow/plugins/{prefix}{name}/'

# datenow_min1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2022, 4, 14, tzinfo=local_tz),
    'email_on_failure': True,
    'email_on_retry': False,
    'email':['duyvq@merapgroup.com', 'vanquangduy10@gmail.com'],
    'do_xcom_push': False,
    'execution_timeout':timedelta(seconds=300)
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=10),
}

dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          schedule_interval= '*/30 8-17,22-22 * * *',
          tags=[prefix+name, 'Daily', '30mins']
)

fdom = datetime.now().replace(day=1).strftime("%Y%m%d")
datenow = datetime.now().strftime("%Y%m%d")
datenow_add1 = (datetime.now() + timedelta(1)).strftime("%Y%m%d")


def update_pending_orders():
    query = f"EXEC pr_OM_RawdataPDAOrderALL_BI '{fdom}', '{datenow}'"
    pending = get_ms_df(sql=query)
    pending['inserted_at2'] =  datetime.now()
    try:
        print("data shape", pending.shape)
        assert pending.shape[0] > 0
    except AssertionError:
        print("No pending orders")
    else:
        print("Do function here")
        execute_bq_query("TRUNCATE TABLE biteam.f_sales_pending")
        bq_values_insert(pending, "f_sales_pending", 2)


# Dont Execute this
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

update_pending_orders = PythonOperator(task_id="update_pending_orders", python_callable=update_pending_orders, dag=dag)

dummy_start >> update_pending_orders