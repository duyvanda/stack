# -*- coding: utf-8 -*-
from utils.df_handle import *
import requests
from vars import vars

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")
name='API_CONTACT'
prefix='CARESOFT_'
path = f'/usr/local/airflow/plugins/{prefix}{name}/'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2021, 10, 1, tzinfo=local_tz),
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
          schedule_interval= '10 0 * * *',
          tags=[prefix+name, 'Daily', 'at0']
)

def get_ids():     
    datenow_min1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    base_url = 'https://api.caresoft.vn/merapgroup/api/v1/'
    headers = {"Authorization": f"Bearer {vars['name']}"}
    extra_url=f'contacts?updated_since={datenow_min1}T00:00:00Z&count=500&page=1'
    r = requests.get(base_url+extra_url, headers=headers)
    data = r.json()['contacts']
    print("rows = ", len(data))
    print("total rows = ",r.json()['numFound'])
    df1=pd.DataFrame.from_records(data)
    try:
        df1.created_at = pd.to_datetime(df1.created_at, dayfirst=True, infer_datetime_format=False).dt.tz_localize(None)
        df1.updated_at = pd.to_datetime(df1.updated_at, dayfirst=True, infer_datetime_format=False).dt.tz_localize(None)
        df1 = df1[['id','updated_at','created_at']].copy()
        df1.to_csv(path+'get_ids_'+datenow_min1+'.csv',index=False)
        execute_values_upsert(df1,'f_caresoft_contactids',pk=["id"])
    except AttributeError as e:
        print(e)

# def first_time_load(pages):
#     for page in pages:
#         base_url = 'https://api.caresoft.vn/merapgroup/api/v1/'
#         headers = {"Authorization": f"Bearer {vars['name']}"}
#         extra_url=f'contacts?updated_since=2022-02-01T00:00:00Z&count=500&page={page}'
#         r = requests.get(base_url+extra_url, headers=headers)
#         data = r.json()['contacts']
#         df1=pd.DataFrame.from_records(data)
#         try:
#             df1.created_at = pd.to_datetime(df1.created_at, dayfirst=True, infer_datetime_format=False).dt.tz_localize(None)
#             df1.updated_at = pd.to_datetime(df1.updated_at, dayfirst=True, infer_datetime_format=False).dt.tz_localize(None)
#             df1 = df1[['id','updated_at','created_at']].copy()
#             execute_values_upsert(df1,'f_caresoft_contactids',pk=["id"])
#         except AttributeError as e:
#             print(e)
# pages = [1,2,3,4,5,6,7,8,9,10]
# first_time_load(pages)

def get_ids_detail():
    '''return a dataframe''' 
    datenow_min1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    df1 = pd.read_csv(path+'get_ids_'+datenow_min1+'.csv', usecols=['id'])
    lst=['id', 'username', 'email', 'email2', 'phone_no', 'phone_no2', 'phone_no3', 'makhdms', 'idpk']
    df = pd.DataFrame(columns=lst)
    Iterators = df1.id.iteritems()
    for i, v in Iterators: 
        base_url = 'https://api.caresoft.vn/merapgroup/api/v1/'
        headers = {"Authorization": f"Bearer {vars['name']}"}
        extra_url=f'contacts/{v}'
        print("url: ",base_url+extra_url)
        try:
            r = requests.get(base_url+extra_url, headers=headers)
            data = r.json()['contact']
            id = data['id']
            username = data['username']
            email = data['email']
            email2 = data['email2']
            phone_no = data['phone_no']
            phone_no2 = data['phone_no2']
            phone_no3 = data['phone_no3']
            makhdms = data['custom_fields'][1]['value']
            data_dict = {
                'id':id,
                'idpk':id,
                'username':username,
                'email':email,
                'email2':email2,
                'phone_no':phone_no,
                'phone_no2':phone_no2,
                'phone_no3':phone_no3,
                'makhdms':makhdms
            }
            df1=pd.DataFrame.from_dict(data_dict, orient='index').T
            df=pd.concat([df,df1], ignore_index=True)
            # execute_values_upsert(df,'f_caresoft_contact_detail',pk=['idpk'])
            print("index ",i)
        except KeyError as e:
            print(e)
            print("index ",i)

    df.to_csv(path+'get_ids_detail_'+datenow_min1+'.csv', index=False)
    execute_values_upsert(df,'f_caresoft_contact_detail',pk=['idpk'])

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

get_ids = PythonOperator(task_id="get_ids", python_callable=get_ids, dag=dag)

get_ids_detail = PythonOperator(task_id="get_ids_detail", python_callable=get_ids_detail, dag=dag)

dummy_start >> get_ids >> get_ids_detail
