from utils.df_handle import *
from nhan.google_service import get_service
import pandas_gbq
from google.cloud import bigquery
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import os


local_tz = pendulum.timezone("Asia/Bangkok")

name='Gdocs'
prefix='D_GiaiTrinh_MDS_'
csv_path = '/usr/local/airflow/plugins'+'/nhan/'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f"{csv_path}spatial-vision-343005-340470c8d77b.json"

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
          schedule_interval= '15 13 * * *',
          tags=[prefix+name, 'Daily', 'at 13h15', 'hanhdt']
)
# Json_file ="D:/ipynb/sever_tttt_ins/datateam1599968716114-6f9f144b4262.json"

service = get_service()

URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQ_2FsvS-u_O2i5bJhd7fJ31G16LfYFe2tvDx4ulAgzcjHHZUAHiNjSu2vyCHthTMv8v-rUmXLolhBS/pub?gid=0&single=true&output=csv'


# spreadsheets_id = '1csSgQ4xamqJx5zbgJJAtSNhjTaLiAZtrAwppEFlzNUA'
# rangeAll = '{0}!A:AA'.format('Check Sale Input')
# body = {}

# database = 'biteam'
# host = '171.235.26.161'
# username = 'biteam'
# password = '123biteam'


# conn = psycopg2.connect(
#     host= host,
#     database= database,
#     user= username,
#     password= password)
def get_data():
    df =pd.read_csv(URL)
    df.columns = cleancols(df)
    df.columns = lower_col(df)
    df['updated_at'] = datetime.now()
    df1=df[['ngay','madh','giaitrinh','sdt','updated_at']]
    return(df1)
def update_psql():
	df2 = get_data()
	pk = ['madh','ngay']
	execute_values_upsert(df2, "d_giaitrinh_mds",pk)

table_name ='d_giaitrinh_mds'
sql = \
f"""
Select * from {table_name}
"""
def update_bq():
    df=get_ps_df(sql)
    pandas_gbq.to_gbq(df, f'biteam.{table_name}', project_id='spatial-vision-343005',if_exists='replace', table_schema = None)

    
def main():
    update_psql()
    update_bq()

if __name__ == "__main__":
    main()
    
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

py_insert_table = PythonOperator(task_id="main", python_callable=main, dag=dag)

dummy_start >> py_insert_table
# >> tab_refresh

