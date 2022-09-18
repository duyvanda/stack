from utils.df_handle import *
from nhan.google_service import get_service
import pandas_gbq
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='DB'
prefix='TTTT_CS'
csv_path = '/usr/local/airflow/plugins/nhan'+'/'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f"{csv_path}spatial-vision-343005-340470c8d77b.json"
table_name ='d_tttt_cs'
sql = \
    f"""
    Select * from {table_name}
    """
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
          schedule_interval= '30 23 * * *',
          tags=[prefix+name, 'Daily', 'at 23:30']
)

def clean_data():

    URL_csinput = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTDuwNudT0lBpAWCziJu-w2gYcuiAdoExpzl2IJx4B8NF32ysVKViwXOVHLOBctrOyyAwT02wZKATiC/pub?gid=405982674&single=true&output=csv'
    df =pd.read_csv(URL_csinput,header=0)
    df.columns = cleancols(df)
    df.columns = lower_col(df)
    df.rename({'codekhmdsmoi':'makhdms','codekhcu':'makhcu'}
            ,axis='columns',inplace =True)
    df_unpivoted = df.melt(id_vars=['makhdms','makhcu','tenkh'], 
                        var_name=['thoigiangoi'], value_name='thongtinthanhtoan')
    df3 = pd.DataFrame(df_unpivoted)
    df3['updated_at'] = datetime.now()
    df3.replace('^\s+', '', regex=True, inplace=True) #front, trim text
    df3.replace('\s+$', '', regex=True, inplace=True) #end
    df4=df3.dropna(subset=['makhdms','thongtinthanhtoan'])
    df5=df4.drop_duplicates(subset=['makhdms','thoigiangoi'],keep='last')
    df6=df5[['makhdms','makhcu','thoigiangoi','thongtinthanhtoan','updated_at']]
    return df6

def update_table(df):
	pk = ['makhdms','thoigiangoi','thongtinthanhtoan']
	execute_values_upsert(df, 'd_tttt_cs',pk)
 

def insert_bigquery():
    try:
        df=get_ps_df(sql)
        pandas_gbq.to_gbq(df, f'biteam.{table_name}', project_id='spatial-vision-343005',if_exists='replace', table_schema = None)
    except:
        raise

def main(): 
    df = clean_data()
    update_table(df=df)
    insert_bigquery()

if __name__ == "__main__":
    main()
    
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

py_main = PythonOperator(task_id="main", python_callable=main, dag=dag)

dummy_start >> py_main