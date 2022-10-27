# %%
from utils.df_handle import *
from requests.structures import CaseInsensitiveDict
import requests
import json
# import base64

# %%
# from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='LOGECOMMERCE'
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
          schedule_interval= '@daily',
          tags=[prefix+name, '@daily']
)

# %%
def insert():    
    url = "https://eoffice.merapgroup.com/eoffice/api/api/raw/log-ecommerce?limit=100000"
    headers = CaseInsensitiveDict()
    headers['Authorization'] = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwOlwvXC8xMjcuMC4wLjE6ODAwMFwvYXBpXC96YWxvXC9kYXRhLWN1c3RvbWVyIiwiaWF0IjoxNjQ3NTA3ODMxLCJleHAiOjE5NTg1NDc4MzEsIm5iZiI6MTY0NzUwNzgzMSwianRpIjoiSlduRjNvcG10a0dEdjBkVSIsInN1YiI6MSwicHJ2IjoiMmFhNjM5ZGEwOTRhNjY4YTQ4NGRkZTJkZjc2NGI5ODg2OTkxMjQ5NiJ9.xtdYIHOiTBdV7Cn_FDSiOTRuPtJ7HD_yjJfE0pZIMw0'
    headers['accept'] = 'application/json'
    headers['content-type'] = 'application/json; charset=UTF-8'
    headers['user-agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    resp = requests.get(url, headers=headers)
    df = pd.DataFrame(resp.json()['data'])
    df.created_at = pd.to_datetime(df.created_at)
    df['inserted_at'] = datetime.now()
    df['pathname_01'] = df.pathname.str.split("/", expand=True)[1]
    df['pathname_02'] = df.pathname.str.split("/", expand=True)[2]
    df['title_page_01'] = df.title_page.str.split("|", expand=True)[0]
    df['title_page_01'].to_clipboard()
    df['title_page_02'] = df.title_page.str.split("|", expand=True)[1]
    dk1 = df['title_page_02'].str.contains('Từ khóa')
    df['title_page_03'] = np.where(dk1, df['title_page_02'], None)
    df['title_page_03'] = df['title_page_03'].str.split(":", expand=True)[1]
    bq_values_insert(df, "f_crawl_logecommerce", 3)

# %%
# df.dtypes

# %%
# insert()

# %%
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

dummy_start >> insert


