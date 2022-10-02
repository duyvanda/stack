from utils.df_handle import *# from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='ACREGIS'
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
          schedule_interval= '0 1 * * *',
          tags=[prefix+name, 'Sync', 'Daily']
)

start_date = '2022-04-01'
# datenow = datetime.now().strftime("%Y-%m-%d")
# datenow_mns1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
# fdom = datetime.now().replace(day=1).strftime("%Y-%m-%d")

from_tb = "OM_AccumulatedRegis"

table_name = "d_accumulatedregis"
table_temp = "d_accumulatedregis_temp"

# sql = \
# f"""
# DECLARE @from DATE = '2022-04-01'
# DECLARE @to DATE = '2022-01-31'
# select
# SlsperID,
# AccumulateID,
# BranchID,
# CustID,
# LevelID,
# [Status],
# Crtd_DateTime,
# Crtd_User,
# Crtd_Prog,
# LUpd_DateTime,
# LUpd_User,
# LUpd_Prog,
# PurchaseAgreementValue,
# PurchaseAgreementID,
# EffectDateNbr
# from {from_tb} where 
# AccumulateID in ('CSBH2204OTC-14QD/MR-KS-STO','CSBH2102PNPP-04QD/PN-Q2')
# and cast(Crtd_DateTime as date) < @from
# and Status = 'C'
# """

# df = get_ms_df(sql)
# df['inserted_at'] = datetime.now()
# bq_values_insert(df, f"{table_temp}", 3)
# sql = \
# f"""
# DROP TABLE IF EXISTS biteam.{table_name};
# CREATE TABLE biteam.{table_name} LIKE biteam.{table_temp}
# PARTITION BY DATE(crtd_datetime)
# CLUSTER BY branchid,custid
# """
# execute_bq_query(sql)

# bq_values_insert(df, f"{table_name}", 2)

usql = \
f"""
DECLARE @from DATE = '{start_date}'
DECLARE @to DATE = '2022-01-31'
select
SlsperID,
AccumulateID,
BranchID,
CustID,
LevelID,
[Status],
Crtd_DateTime,
Crtd_User,
Crtd_Prog,
LUpd_DateTime,
LUpd_User,
LUpd_Prog,
PurchaseAgreementValue,
PurchaseAgreementID,
EffectDateNbr
from {from_tb} where 
AccumulateID in ('CSBH2204OTC-14QD/MR-KS-STO','CSBH2102PNPP-04QD/PN-Q2')
and cast(Crtd_DateTime as date) >= @from
and Status = 'C'
"""


# table_name = "sync_dms_dv"
# table_temp = "sync_dms_dv_temp"

def insert():
    print(usql)

def update():
    # print(sql)
    df = get_ms_df(usql)
    df['inserted_at'] = datetime.now()

    dsql = \
    f"""
    delete from biteam.{table_name} where date(crtd_datetime) >= '{start_date}'
    """
    print("delete_sql: ", dsql)
    execute_bq_query(dsql)

    bq_values_insert(df, f"{table_name}", 2)

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

dummy_end = DummyOperator(task_id="dummy_end", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

update = PythonOperator(task_id="update", python_callable=update, dag=dag)

dummy_start >> insert >> update >> dummy_end
