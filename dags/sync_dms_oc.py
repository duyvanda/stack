from ast import Try
from utils.df_handle import *

# from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='OC'
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

# start_date = '2022-01-01'
datenow = datetime.now().strftime("%Y-%m-%d")
# datenow_mns1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
# fdom = datetime.now().replace(day=1).strftime("%Y-%m-%d")
datenow_mns45 = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")
x_date = datenow if datetime.now().hour in {7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23} else datenow_mns45

from_tb = "PPC_OutsideChecking"

table_name = "sync_dms_oc"
table_temp = "sync_dms_oc_temp"

sql = \
f"""
DECLARE @from DATE = '{x_date}'
DECLARE @cc VARCHAR(100)= (SELECT TextVal FROM vs_SYS_Configurations WHERE Code='ImagePublic')
DECLARE @to DATE = '2022-01-31'
select
SlsperID,
CustID,
VisitDate,
NoteID,
BranchID,
oc.Note,
rs.Descr,
SalesID,
Distance,
CheckInType = CASE WHEN oc.NoteID = 2 THEN N'Giao Hàng' WHEN oc.NoteID = 1 THEN N'Thu Nợ' ELSE N'Bán Hàng' end,
ImageFileName = CASE WHEN oc.ImageFileName ='' THEN '' ELSE @cc+oc.ImageFileName END
from PPC_OutsideChecking oc
LEFT JOIN dbo.OM_ReasonCodePPC rs WITH (NOLOCK) ON oc.ReasonCode=rs.Code AND rs.Type IN ('DELIDISTANCE','DS','REDEBTDISTANCE')
where cast(oc.VisitDate as DATE) >= @from
"""


def insert():
    print(sql)

def update():

    df = get_ms_df(sql)
    print("df shape", df.shape)
    try:
        assert df.shape[0] > 0
        dsql = \
        f"""
        delete from biteam.{table_name} where date(visitdate) >= '{x_date}'
        """
        print("delete_sql: ", dsql)
        execute_bq_query(dsql)
        df['inserted_at'] = datetime.now()
        bq_values_insert(df, f"{table_name}", 2)
    except AssertionError:
        print("No new data")

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

update = PythonOperator(task_id="update", python_callable=update, dag=dag)

dummy_start >> insert >> update