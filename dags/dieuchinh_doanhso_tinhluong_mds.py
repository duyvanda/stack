from utils.df_handle import *
from nhan.google_service import get_service
from google.cloud import bigquery
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import os
import pandas_gbq
import numpy

service = get_service()
bigqueryClient = bigquery.Client()

local_tz = pendulum.timezone("Asia/Bangkok")

name='_Tinhluong'
prefix='MDS_Dieuchinh_DS'
csv_path = '/usr/local/airflow/plugins'+'/nhan/'
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f"{csv_path}spatial-vision-343005-340470c8d77b.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/usr/local/airflow/dags/files/bigquery2609.json"

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
        #   https://crontab.guru/
        #   @once
          schedule_interval= '0 4 2,3,4,5,6 * *',
          tags=[prefix+name, 'Daily', 'at 5h15','hanhdt']
)

def data_chinhsua_tt():
    # Chỉnh sửa thông tin tính lương
    URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQ-yoTsTaGUDq8K6Qu5HldZr30DRaTid6OT92VSI24UDagVuBYJd7Rv-yqdaGXqdP4H_cKlMLl0rsJ_/pub?gid=0&single=true&output=csv'
    df =pd.read_csv(URL,parse_dates=['THÁNG'])
    df.columns = cleancols(df)
    df.columns = lower_col(df)
    df['inserted_at'] = datetime.now()
    # df['updated_at'] = datetime.now()
    df.rename({'noi':'magop','moi':'dieuchinh'}
          ,axis='columns',inplace =True)
    df.replace('^\s+', '', regex=True, inplace=True) #front, trim text
    df.replace('\s+$', '', regex=True, inplace=True) #end
    df1=df
    return df1

def insert_bq_dieuchinh(df):
    table_name ='d_chinhsua_tt_mds'
    pandas_gbq.to_gbq(df, f'biteam.{table_name}', project_id='spatial-vision-343005',if_exists='replace', table_schema = None)
    print('Done')

def data_guikho_ins():
    # Gửi kho hàng thầu (Trừ doanh số theo tháng)
    URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQ-yoTsTaGUDq8K6Qu5HldZr30DRaTid6OT92VSI24UDagVuBYJd7Rv-yqdaGXqdP4H_cKlMLl0rsJ_/pub?gid=784301745&single=true&output=csv'
    df =pd.read_csv(URL,dtype={'Số Lô':object},parse_dates=['THÁNG'])
    df.columns = cleancols(df)
    df.columns = lower_col(df)
    df['soluongmoi'] = df['soluongmoi'].astype(str)
    df['doanhsochuavatmoi']=df['doanhsochuavatmoi'].str.replace(',','')
    df['doanhsocovatmoi']=df['doanhsocovatmoi'].str.replace(',','')
    df['soluongmoi']=df['soluongmoi'].str.replace(',','')
    df['doanhsochuavatmoi']=df['doanhsochuavatmoi'].astype(int)
    df['doanhsocovatmoi']=df['doanhsocovatmoi'].astype(int)
    df['soluongmoi']=df['soluongmoi'].astype(int)
    df['inserted_at'] = datetime.now()
    # df['updated_at'] = datetime.now()
    df.rename({'stt':'loai_dulieu'}
            ,axis='columns',inplace =True)
    df.replace('^\s+', '', regex=True, inplace=True) #front, trim text
    df.replace('\s+$', '', regex=True, inplace=True) #end
    df1=df
    return df1

def insert_bq_guikho(df):
    table_name ='d_guikho_hangthau_mds'
    pandas_gbq.to_gbq(df, f'biteam.{table_name}', project_id='spatial-vision-343005',if_exists='replace', table_schema = None)
    print('Done')

def data_xuatkho_ins():
    # Xuất kho hàng thầu (cộng doanh số theo tháng)
    URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQ-yoTsTaGUDq8K6Qu5HldZr30DRaTid6OT92VSI24UDagVuBYJd7Rv-yqdaGXqdP4H_cKlMLl0rsJ_/pub?gid=772376095&single=true&output=csv'
    df =pd.read_csv(URL,dtype={'Số Lô':object},parse_dates=['THÁNG'])
    df.columns = cleancols(df)
    df.columns = lower_col(df)
    df['soluongmoi'] = df['soluongmoi'].astype(str)
    df['doanhsochuavatmoi']=df['doanhsochuavatmoi'].str.replace(',','')
    df['doanhsocovatmoi']=df['doanhsocovatmoi'].str.replace(',','')
    df['soluongmoi']=df['soluongmoi'].str.replace(',','')
    df['doanhsochuavatmoi']=df['doanhsochuavatmoi'].astype(int)
    df['doanhsocovatmoi']=df['doanhsocovatmoi'].astype(int)
    df['soluongmoi']=df['soluongmoi'].astype(int)
    df['inserted_at'] = datetime.now()
    # df['updated_at'] = datetime.now()
    df.rename({'stt':'loai_dulieu'}
              ,axis='columns',inplace =True)
    df.replace('^\s+', '', regex=True, inplace=True) #front, trim text
    df.replace('\s+$', '', regex=True, inplace=True) #end
    df1=df
    return df1

def insert_bq_xuatkho(df):
    table_name ='d_xuatkho_hangthau_mds'
    pandas_gbq.to_gbq(df, f'biteam.{table_name}', project_id='spatial-vision-343005',if_exists='replace', table_schema = None)
    print('Done')
def main():
    df=data_chinhsua_tt()
    insert_bq_dieuchinh(df)
    df1=data_guikho_ins()
    insert_bq_guikho(df1) 
    df2=data_xuatkho_ins()
    insert_bq_xuatkho(df2)

if __name__ == "__main__":
    main()
    
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

py_main= PythonOperator(task_id="main", python_callable=main, dag=dag)

dummy_start >> py_main
# >> tab_refresh