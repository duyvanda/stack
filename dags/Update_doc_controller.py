from utils.df_handle import *
from nhan.google_service import get_service
from google.cloud import bigquery
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import os
import pandas_gbq



local_tz = pendulum.timezone("Asia/Bangkok")
table_name ='d_master_docs_controller'
bigqueryClient = bigquery.Client()

name='controller'
prefix='Update_doc_'
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
          schedule_interval= '0 8-20 * * *',
          tags=[prefix+name, 'Hourly', 'at minute 0, 8-20']
)

service = get_service()

def data_doc():
    URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vS_SqGlVlQObBecB9isKfAFEtWZpacuNrrMfh8uj1KeFnYNS_J7o_kxiFvirhf1BXOsI3OKd626uwkc/pub?gid=781077763&single=true&output=csv'
    df =pd.read_csv(URL,parse_dates=['Hiệu lực từ','Hiệu lực đến','Ngày ký'])
    df.columns = cleancols(df)
    df.columns = lower_col(df)
    df['updated_at'] = datetime.now()
    df1=df
    return (df1)

def insert_bq():
    df=data_doc()
    pandas_gbq.to_gbq(df, f'biteam.{table_name}', project_id='spatial-vision-343005',if_exists='replace', table_schema = None)

def main():
    insert_bq()

if __name__ == "__main__":
    main()
    
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

py_main= PythonOperator(task_id="main", python_callable=main, dag=dag)

dummy_start >> py_main
# >> tab_refresh