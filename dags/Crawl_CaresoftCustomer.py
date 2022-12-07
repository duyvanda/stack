# %%
from utils.df_handle import *
import requests
from requests.structures import CaseInsensitiveDict
import pickle

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
    'execution_timeout':timedelta(seconds=300),
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          schedule_interval= '@daily',
          tags=[prefix+name, 'daily']
)

# %%
data1 = {"from":1643652037221,"to":1656608154405}
data2 = {"from": 1656612054988, "to": 1669827540000}
data3 = {"from": 1669827654768, "to": 1672505961391}
list_dict = [data1, data2, data3]
list_pickle_pathname = [csv_path+"part1.pickle", csv_path+"part2.pickle", csv_path+"part3.pickle"]

def get_ck():
    url = "https://web11.caresoft.vn/api/login/auth"
    headers = CaseInsensitiveDict()
    headers['acid'] = '8421'
    headers['content-type'] = 'application/json;charset=UTF-8'
    headers['domain'] = 'merapgroup'
    data = \
    {
        "email": "cskh@merapgroup.com",
        "password": "Merap@123",
        "account_id": "8421"
    }
    resp = requests.post(url, headers=headers, json=data)
    ck = dict(resp.headers)['Set-Cookie']
    with open('token.string','wb') as f:
        f.write(pickle.dumps(ck))
    return None


def get_data(pickle_pathname: str, data: dict):
    
    with open('token.string','rb') as f:
        ck = pickle.load(f)
    
    print("ck", ck)
    
    url = "https://web11.caresoft.vn/admin/user/export"
    headers = CaseInsensitiveDict()
    headers['authority'] = 'web11.caresoft.vn'
    headers['accept'] = 'application/json, text/plain, */*'
    headers['content-type'] = 'application/json;charset=UTF-8'
    headers['cookie'] = ck
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
    df4 = pd.read_pickle(csv_path+"part3.pickle")
    df3 = union_all([df3,df4])
    print(df3.columns)
    bq_values_insert(df3, "d_caresoft_customer", 3)



# %% [markdown]
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

dummy_end = DummyOperator(task_id="dummy_end", dag=dag)

get_ck = PythonOperator(task_id="get_ck", python_callable=get_ck, dag=dag)

get_pickle = PythonOperator(task_id="get_pickle", python_callable=get_pickle, dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

dummy_start >> get_ck >> get_pickle >> insert >> dummy_end


