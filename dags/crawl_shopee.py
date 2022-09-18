import requests
import pandas as pd
from requests.structures import CaseInsensitiveDict
from utils.df_handle import *
from nhan.google_service import get_service
import pandas_gbq
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from datetime import datetime 
import os
import numpy as np
import time
from datetime import datetime
import json




local_tz = pendulum.timezone("Asia/Bangkok")

name='DB'
prefix='Crawl_Shopee'
csv_path = '/usr/local/airflow/plugins/nhan'+'/'


dag_params = {
    'owner': 'nhanvo',
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
          # https://crontab.guru/
          # @once 0 0 * * 1,3,5
          schedule_interval= '00 0-23 * * *',
          tags=[prefix+name, 'Hourly']
)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f"{csv_path}spatial-vision-343005-340470c8d77b.json"
table_name ='d_crawl_shopee'
bigqueryClient = bigquery.Client()
now = datetime.now() # current date and time
date_time = now.strftime("%m-%d-%Y_%H-%M")


# url="""https://shopee.vn/api/v4/recommend/recommend?bundle=shop_page_main&item_card=2&limit=10&offset=0&shopid=799716829"""
# headers = CaseInsensitiveDict()
# headers['sec-ch-ua'] = '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"'
# headers['sec-ch-ua-mobile'] = '?1'
# headers['User-Agent'] = 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Mobile Safari/537.36'
# headers['X-API-SOURCE'] = 'rweb'
# headers['X-Shopee-Language'] = 'vi'
# headers['Referer'] = 'https://shopee.vn/merap'
# headers['X-Requested-With'] = 'XMLHttpRequest'
# headers['If-None-Match-'] = '55b03-2aa3ea81e1919a1ada06fd103fb6ff60'
# headers['sec-ch-ua-platform'] = '"Android"'
# # headers['']
# # headers['']
# # headers['']
# # headers['']
# resp = requests.get(url, headers=headers)
# #link mở file hình : https://cf.shopee.vn/file/


# data=resp.json()

# data1 =data['data']['sections'][0]['data']['item']
# df = pd.DataFrame(data1)
# value_check=int(df['key'].count())
# value_check_total =int(data['data']['sections'][0]['total'])

# while value_check != value_check_total:
#     resp = requests.get(url, headers=headers)
#     data=resp.json()
#     data2 =data['data']['sections'][0]['data']['item']
#     df = pd.DataFrame(data1)
#     a=int(df['key'].count())
#     b =int(data['data']['sections'][0]['total'])
#     print('dem khac 10')
#     if a == b:
#         break
# else:
#     df = pd.DataFrame(data1)
#     df['price']=df['price'] /100000
#     df['price_min']=df['price_min'] /100000
#     df['price_max']=df['price_max'] /100000
#     df['sku'] =df['key'].str.replace('item::','')
#     df['image'] = 'https://cf.shopee.vn/file/' + df['image']
#     df['updated_at']=datetime.now()

#     data_rating =df['item_rating'].to_dict()

#     df_rating =pd.DataFrame.from_dict(data_rating).transpose()

#     df_rating1=df_rating[['rating_star','rcount_with_image','rcount_with_context']]

#     df1= df[[
#         'shopid','sku','name','price','price_min','price_max','show_discount','status','stock','sold','historical_sold','liked_count',
#         'cmt_count','image'
#     ]]

#     df2=df1.join(df_rating1)
#     df2_final=df2.dropna(subset=['sku'])

def get_link():
    url="""https://shopee.vn/api/v4/recommend/recommend?bundle=shop_page_main&item_card=2&limit=10&offset=0&shopid=799716829"""
    headers = CaseInsensitiveDict()
    headers['sec-ch-ua'] = '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"'
    headers['sec-ch-ua-mobile'] = '?1'
    headers['User-Agent'] = 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Mobile Safari/537.36'
    headers['X-API-SOURCE'] = 'rweb'
    headers['X-Shopee-Language'] = 'vi'
    headers['Referer'] = 'https://shopee.vn/merap'
    headers['X-Requested-With'] = 'XMLHttpRequest'
    headers['If-None-Match-'] = '55b03-2aa3ea81e1919a1ada06fd103fb6ff60'
    headers['sec-ch-ua-platform'] = '"Android"'
    # headers['']
    # headers['']
    # headers['']
    # headers['']
    resp = requests.get(url, headers=headers)
    data=resp.json()
    return data
    #link mở file hình : https://cf.shopee.vn/file/

# def check_data():
#     data =get_link()
#     data1 =data['data']['sections'][0]['data']['item']
#     df = pd.DataFrame(data1)
#     value_check=int(df['key'].count())
#     value_check_total =int(data['data']['sections'][0]['total'])
#     while value_check != value_check_total:
#         data_check=get_link()
#         data2 =data_check['data']['sections'][0]['data']['item']
#         df_2 = pd.DataFrame(data2)
#         a=int(df_2['key'].count())
#         b =int(data_check['data']['sections'][0]['total'])
#         print(a)
#         print(b)
#         if a == b:
#             return data_check
#             break
#     else:
#         return data

list_check = [1,2,3,4,5]
def check_data():
    # while value_check != value_check_total:
    for i in list_check:
        try:
            print(i)
            data = get_link()
            data1 =data['data']['sections'][0]['data']['item']
            df = pd.DataFrame(data1)
            value_check=int(df['key'].count())
            value_check_total =int(data['data']['sections'][0]['total'])
            print(value_check,value_check_total)       
            assert value_check == value_check_total
            # data1 = data
            break
        except AssertionError:
            time.sleep(5)
            # data = get_link()
        except:
            time.sleep(5)
            # data = get_link()
        # finally:
            # print("Het block try")
    return data

def saving_json(jsonfile):
    json_object = json.dumps(jsonfile)
    data1 =jsonfile['data']['sections'][0]['data']['item']
    df = pd.DataFrame(data1)
    value_check=df['key'].count()
    value_check_total =jsonfile['data']['sections'][0]['total']
    # print(json_object)
    with open(f"{csv_path}crawl_shopee/data_crawl_shopee_{date_time}_item-{value_check}_total-{value_check_total}.json", "w") as outfile:
        outfile.write(json_object)



# data = check_data()
def get_data(data):
    
    data1 =data['data']['sections'][0]['data']['item']
    df = pd.DataFrame(data1)
    df['price']=df['price'] /100000
    df['price_min']=df['price_min'] /100000
    df['price_max']=df['price_max'] /100000
    df['sku'] =df['key'].str.replace('item::','')
    df['image'] = 'https://cf.shopee.vn/file/' + df['image']
    df['updated_at']=datetime.now()

    data_rating =df['item_rating'].to_dict()

    df_rating =pd.DataFrame.from_dict(data_rating).transpose()

    df_rating1=df_rating[['rating_star','rcount_with_image','rcount_with_context']]

    df1= df[[
            'shopid','sku','name','price','price_min','price_max','show_discount','status','stock','sold','historical_sold','liked_count',
            'cmt_count','image'
        ]]

    df2=df1.join(df_rating1)
    df2_final=df2.dropna(subset=['sku'])
    return df2_final





def update_table(df_update):
    
	pk = ['sku','shopid','inserted_at']
	execute_values_upsert(df_update, "d_crawl_shopee",pk)

sql_bq = f'''
Select max(inserted_at) as inserted_at from biteam.{table_name}
'''
def insert_bq():
    df_bq = bigqueryClient.query(sql_bq).to_dataframe()
    
    value =df_bq['inserted_at'].values[0]
    
    timestamp = ((value - np.datetime64('1970-01-01T00:00:00'))
                     / np.timedelta64(1, 's'))
    
    max_timestamp=datetime.utcfromtimestamp(timestamp)
    
    sql_psql =\
        f"""
        SELECT * FROM {table_name} where inserted_at > '{max_timestamp}'
        """
    
    df_psql=get_ps_df(sql_psql)
    pandas_gbq.to_gbq(df_psql, f'biteam.{table_name}', project_id='spatial-vision-343005',if_exists='append', table_schema = None)



def main(): 
    data = check_data()
    saving_json(jsonfile=data)
    df_final = get_data(data=data)
    update_table(df_final)
    insert_bq()

if __name__ == "__main__":
    main()



dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

# check_data = PythonOperator(task_id="check_data", python_callable=check_data, dag=dag)

# get_data = PythonOperator(task_id="get_data", python_callable=get_data, dag=dag)

py_main = PythonOperator(task_id="main", python_callable=main, dag=dag)

dummy_start >> py_main
