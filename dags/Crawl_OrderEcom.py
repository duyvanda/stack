from utils.df_handle import *
from requests.structures import CaseInsensitiveDict
import requests
import json
import pickle

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='ORDER_ECOM'
prefix='CRAWL_'
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
          schedule_interval= '*/30 7-23 * * *',
          tags=[prefix+name, '30min']
)


def get_date():
    df_lst_date = get_bq_df(
    """
    select distinct cast( date(created_at) as string) as date_sync from `biteam.f_crawl_orderecommerce`
    where process_name not in ("Đã giao hàng", "Yêu cầu hủy", "Đóng đơn hàng")
    """
    )
    # df.date_sync.to_list()
    datenow = datetime.now().strftime("%Y-%m-%d")
    lst = df_lst_date.date_sync.to_list()
    lst.append(datenow)
    set_lst = set(lst)
    with open('set_lst.set','wb') as f:
        f.write(pickle.dumps(set_lst))
    return None


def get_data():
    
    with open('set_lst.set','rb') as f:
        set_lst = pickle.load(f)
    
    df0 = pd.DataFrame()
    for l in set_lst:
        url = f"https://eoffice.merapgroup.com/eoffice/api/api/raw/list-order?date_start={l}&date_end={l}&limit=10000"
        headers = CaseInsensitiveDict()
        headers['Authorization'] = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwOlwvXC8xMjcuMC4wLjE6ODAwMFwvYXBpXC96YWxvXC9kYXRhLWN1c3RvbWVyIiwiaWF0IjoxNjQ3NTA3ODMxLCJleHAiOjE5NTg1NDc4MzEsIm5iZiI6MTY0NzUwNzgzMSwianRpIjoiSlduRjNvcG10a0dEdjBkVSIsInN1YiI6MSwicHJ2IjoiMmFhNjM5ZGEwOTRhNjY4YTQ4NGRkZTJkZjc2NGI5ODg2OTkxMjQ5NiJ9.xtdYIHOiTBdV7Cn_FDSiOTRuPtJ7HD_yjJfE0pZIMw0'
        headers['accept'] = 'application/json'
        headers['content-type'] = 'application/json; charset=UTF-8'
        headers['user-agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
        resp = requests.get(url, headers=headers)
        df = pd.DataFrame(resp.json()['data'])
        df0 = union_all([df0, df])
    df0['inserted_at'] = datetime.now()
    df0.created_at = pd.to_datetime(df0.created_at)
    df0.delivery_date = pd.to_datetime(df0.delivery_date)
    bq_values_insert(df0, "f_crawl_orderecommerce_temp", 3)
    return None


def upsert():
    sql = \
    """
    MERGE biteam.f_crawl_orderecommerce T
    USING biteam.f_crawl_orderecommerce_temp S
    ON T.code_id = S.code_id
    WHEN MATCHED THEN
    UPDATE SET
    T.code_dms = S.code_dms,
    T.type_sync = S.type_sync,
    T.date_sync = S.date_sync,
    T.created_at = S.created_at,
    T.delivery_date = S.delivery_date,
    T.customer_code = S.customer_code,
    T.customer_name = S.customer_name,
    T.total_amount = S.total_amount,
    T.process_name = S.process_name
    WHEN NOT MATCHED THEN
    INSERT (code_id, code_dms, type_sync, date_sync, created_at, delivery_date, customer_code, customer_name, total_amount, process_name, inserted_at) 
    VALUES (code_id, code_dms, type_sync, date_sync, created_at, delivery_date, customer_code, customer_name, total_amount, process_name, inserted_at)
    """

    execute_bq_query(sql)


def mass_update():
    dk = datetime.now().hour in {23} and datetime.now().minute < 30
    datenow = datetime.now().strftime("%Y-%m-%d")
    datemn_90 = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    if dk:
        url = f"https://eoffice.merapgroup.com/eoffice/api/api/raw/list-order?date_start={datemn_90}&date_end={datenow}&limit=10000"
        headers = CaseInsensitiveDict()
        headers['Authorization'] = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwOlwvXC8xMjcuMC4wLjE6ODAwMFwvYXBpXC96YWxvXC9kYXRhLWN1c3RvbWVyIiwiaWF0IjoxNjQ3NTA3ODMxLCJleHAiOjE5NTg1NDc4MzEsIm5iZiI6MTY0NzUwNzgzMSwianRpIjoiSlduRjNvcG10a0dEdjBkVSIsInN1YiI6MSwicHJ2IjoiMmFhNjM5ZGEwOTRhNjY4YTQ4NGRkZTJkZjc2NGI5ODg2OTkxMjQ5NiJ9.xtdYIHOiTBdV7Cn_FDSiOTRuPtJ7HD_yjJfE0pZIMw0'
        headers['accept'] = 'application/json'
        headers['content-type'] = 'application/json; charset=UTF-8'
        headers['user-agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
        resp = requests.get(url, headers=headers)
        df = pd.DataFrame(resp.json()['data'])
        df['inserted_at'] = datetime.now()
        df.created_at = pd.to_datetime(df.created_at)
        df.delivery_date = pd.to_datetime(df.delivery_date)
        execute_bq_query(f""" DELETE FROM biteam.f_crawl_orderecommerce where date(created_at) >='{datemn_90}' """)
        bq_values_insert(df, "f_crawl_orderecommerce", 2)
    else:
        print("NOT A GOOD TIME")
        print(datetime.now().hour, datetime.now().minute)


dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

get_date = PythonOperator(task_id="get_date", python_callable=get_date, dag=dag)
get_data = PythonOperator(task_id="get_data", python_callable=get_data, dag=dag)
upsert = PythonOperator(task_id="upsert", python_callable=upsert, dag=dag)
mass_update = PythonOperator(task_id="mass_update", python_callable=mass_update, dag=dag)

dummy_start >> get_date >> get_data >> upsert >> mass_update
