from utils.df_handle import *

# from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='RD'
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
          schedule_interval= '*/30 8-17,23-23 * * *',
          tags=[prefix+name, 'Sync', '30mins']
)

start_date = '2022-01-01'
datenow = datetime.now().strftime("%Y-%m-%d")
datenow_mns1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
fdom = datetime.now().replace(day=1).strftime("%Y-%m-%d")

from_tb1 = "OM_Receipt"
from_tb2 = "OM_ReceiptDet"

table_name = "sync_dms_rd"
table_temp = "sync_dms_rd_temp"

sql = \
f"""
select
r.BranchID,
rd.OrderNbr,
r.ReportID,
r.CustID,
r.Status,
r.Comment,
r.IssueDate,
r.Crtd_DateTime,
r.Crtd_Prog,
r.Crtd_User,
r.Lupd_DateTime,
r.Lupd_User,
r.DeliveryUnit,
r.TruckID
from {from_tb2} as rd
INNER JOIN {from_tb1} as r ON
r.ReportID = rd.ReportID and
r.BranchID = rd.BranchID
"""

# df = get_ms_df(sql)
# df['inserted_at'] = datetime.now()
# bq_values_insert(df, f"{table_temp}", 3)
# sql = \
# f"""
# DROP TABLE IF EXISTS biteam.{table_name};
# CREATE TABLE biteam.{table_name} LIKE biteam.{table_temp}
# PARTITION BY DATE(crtd_datetime)
# CLUSTER BY branchid,ordernbr,custid
# """
# execute_bq_query(sql)

# bq_values_insert(df, f"{table_name}", 2)

def insert():
    pass

def update():
    df = get_ms_df(sql)
    df['inserted_at'] = datetime.now()
    try:
        print("data shape", df.shape)
        assert df.shape[0] >0
    except AssertionError:
        print("Has data")
    else:
        bqsql = \
        f"""truncate table biteam.{table_name}"""
        execute_bq_query(bqsql)
        bq_values_insert(df, f"{table_name}", 2)

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

dummy_end = DummyOperator(task_id="dummy_end", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

update = PythonOperator(task_id="update", python_callable=update, dag=dag)

dummy_start >> insert >> update >> dummy_end
