# %%
from utils.df_handle import *
import requests
from requests.structures import CaseInsensitiveDict

# %% [markdown]
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='CareSoftCustomer'
prefix='Crawl_'
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
          tags=[prefix+name, 'daily']
)

# %%
data1 = {"from":1643652037221,"to":1656608154405}
data2 = {"from": 1656612054988, "to": 1672505977905}
list_dict = [data1, data2]
list_pickle_pathname = [csv_path+"part1.pickle", csv_path+"part2.pickle"]

def get_data(pickle_pathname: str, data: dict):
    url = "https://web11.caresoft.vn/admin/user/export"
    headers = CaseInsensitiveDict()
    headers['authority'] = 'web11.caresoft.vn'
    headers['accept'] = 'application/json, text/plain, */*'
    headers['content-type'] = 'application/json;charset=UTF-8'
    headers['cookie'] = '_fbp=fb.1.1663812395640.118225221; _ga=GA1.1.296298468.1663812395; remember_82e5d2c56bdd0811318f0cf078b78bfc=eyJpdiI6ImEzOVhXYUpKZ0xaTzJWbWtPXC9rVGNnPT0iLCJ2YWx1ZSI6Ikt1ZnlYVVJldzJwMllQTUhpVkVcL0dnWHV6dG1UKzl1VWFCN2gxTm1ka0UrXC9LOWpsM3VzNDZyeEtkXC9oTW83MHlaQ0dKU3NkQU5tUmdNZ2VOYVgrS0hkSGdOcEFKdWRGa1Z0OTdaR0VZa293PSIsIm1hYyI6ImU2OWVlMzU3MWM5OTExZjc3NGRmYWZkZmMyZjU3NzQ5ODVjODMwMGE2ODMwOGRmMDlmYWY1MWRkZWYxNzhlOTkifQ%3D%3D; _ga_TXHEXQXK1P=GS1.1.1666680497.4.1.1666682046.0.0.0; laravel_session=eyJpdiI6IndqSWdlV2ZtQTZjdUJiM3RKOXlRM2c9PSIsInZhbHVlIjoiZ2ZrSmZscERIaVBMekM3MXlVa08za2xKWmJhR3dDMEFQOTBrMXcreitXQWxBSEhyaHFIUFA3WUExd002cXpTbG5RYnhiZUN2b3hNSGJXZkRubHJLU1E9PSIsIm1hYyI6IjdiYzc0MTFkNzExZDlkMWQxNGI5M2RmZDQ4MjE1N2FiOWVhOWM4ZDY2ZmM4NWM0YTdkNTg2M2Y5ZDRhMmFhYmEifQ%3D%3D'
    headers['user-agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    resp = requests.post(url, headers=headers, json=data)
    print("resp ", resp.json())
    time.sleep(2)   
    df = pd.read_excel(resp.json()['status']['extraData']['urlToFile'], dtype={'Số điện thoại chính': str, 'Số điện thoại phụ 1': str})
    df.columns = cleancols(df)
    df.columns = lower_col(df)
    df.thoidiemcapnhat = pd.to_datetime(df.thoidiemcapnhat)
    df.thoidiemtao = pd.to_datetime(df.thoidiemtao)
    df.to_pickle(pickle_pathname)

# %%
def get_pickle():
    zipped = zip(list_dict, list_pickle_pathname)
    for tuple in zipped:
        print(tuple[0], tuple[1])
        get_data(tuple[1], tuple[0])

def insert():
    df1 = pd.read_pickle(csv_path+"part1.pickle")
    # bq_values_insert(df, "d_caresoft_customer")
    df2 = pd.read_pickle(csv_path+"part2.pickle")
    # bq_values_insert(df, "d_caresoft_customer")
    df3 = union_all([df1,df2])
    print(df3.columns)
    bq_values_insert(df3, "d_caresoft_customer", 3)



# %% [markdown]
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

dummy_end = DummyOperator(task_id="dummy_end", dag=dag)

get_pickle = PythonOperator(task_id="get_pickle", python_callable=get_pickle, dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

dummy_start >> get_pickle >> insert >> dummy_end


