from utils.df_handle import *

# from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='SACHECKIN'
prefix='SYNC_'
csv_path = '/usr/local/airflow/plugins'+'/'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2022, 5, 10, tzinfo=local_tz),
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
          schedule_interval= '*/30 6-23 * * *',
          tags=[prefix+name, 'Sync', 'Daily']
)

from_tb1 = "AR_SalespersonLocationTrace"
# from_tb2 = "OM_DeliReportDet"
table_name = "sync_dms_sacheckin"
table_temp = "sync_dms_sacheckin_temp"

# start_date = '2022-01-01'
datenow = datetime.now().strftime("%Y-%m-%d")
# datenow_mns1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
# fdom = datetime.now().replace(day=1).strftime("%Y-%m-%d")
datenow_mns45 = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")
x_date = datenow if datetime.now().hour in {7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23} else datenow_mns45

sql = \
f"""
DECLARE @from DATE = '{x_date}'
SELECT
BranchID,
SlsperID,
OrderNbr as SaOrderNbr,
UpdateTime as SA_UpdateTime,
NumberCICO,
OrdAmt
from
dbo.{from_tb1}
where CAST (UpdateTime as DATE) >= @from
--and CAST (UpdateTime as DATE) <= @to
and SUBSTRING(Type,1,2) = 'SA'
"""

# df = get_ms_df(sql)
# df['inserted_at'] = datetime.now()
# bq_values_insert(df, f"{table_temp}", 3)
# sql = \
# f"""
# DROP TABLE IF EXISTS biteam.{table_name};
# CREATE TABLE biteam.{table_name} LIKE biteam.{table_temp}
# CLUSTER BY branchid,numbercico,saordernbr
# """
# execute_bq_query(sql)

# sql = \
# f"""
# DROP TABLE IF EXISTS biteam.{table_name};
# CREATE TABLE biteam.{table_name} LIKE biteam.{table_temp}
# CLUSTER BY branchid,reportid,batnbr,ordernbr
# """
# execute_bq_query(sql)

# print(sql)

# bq_values_insert(df, f"{table_name}", 2)

def insert():
    print(sql)

def update():

    df = get_ms_df(sql)
    print("df shape", df.shape)
    try:
        assert df.shape[0] > 0
        dsql = \
        f"""
        delete from biteam.{table_name} where date(sa_updatetime) >= '{x_date}'
        """
        print("delete_sql: ", dsql)
        execute_bq_query(dsql)
        df['inserted_at'] = datetime.now()
        df.to_csv(f'{csv_path}{prefix}{name}/file.csv', index=False)
        bq_values_insert(df, f"{table_name}", 2)

        # create file sensor
        with open(csv_path+f'FILESENSOR/{prefix+name}.txt','w') as f:
            f.close()
    except AssertionError:
        print("No data to input")


dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

dummy_end = DummyOperator(task_id="dummy_end", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

update = PythonOperator(task_id="update", python_callable=update, dag=dag)

dummy_start >> insert >> update >> dummy_end
