# -*- coding: utf-8 -*-

from utils.df_handle import *

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='TraceLocation'
prefix='DMS_'
csv_path = '/usr/local/airflow/plugins'+'/'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2022, 4, 4, tzinfo=local_tz),
    'email_on_failure': True,
    'email_on_retry': False,
    'email':['duyvq@merapgroup.com', 'vanquangduy10@gmail.com'],
    'do_xcom_push': False,
    'execution_timeout':timedelta(seconds=300)
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=10),
}

dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          schedule_interval= '5 0 * * *',
          tags=[prefix+name, 'DEAD', 'at5']
)


datenow = datetime.now().strftime("%Y-%m-%d")
datenow_mns1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
fdom = datetime.now().replace(day=1).strftime("%Y%m%d")

# datenow_mns1 = '20220331'
# datenow_mns2 = '20220403'
sql = \
f"""
WITH T1 as
(
SELECT
BranchID,
SlsperID,
Lat,
Lng,
CustID,
Type as Typ,
NumberCICO,
UpdateTime as CheckInUpdateTime
from dbo.AR_SalespersonLocationTrace 
where CAST (UpdateTime as DATE) >= '{datenow_mns1}'
and CAST (UpdateTime as DATE) <= '{datenow_mns1}'
and Type = 'IO'
--ORDER BY UpdateTime ASC
)

--select top 100 SUBSTRING(Type,1,2), Type from dbo.AR_SalespersonLocationTrace where CAST (UpdateTime as DATE) = '2022-03-25'
, T2 as
(
SELECT
BranchID,
SlsperID,
SUBSTRING(Type,3,20) as DeOrderNbr,
UpdateTime as DE_UpdateTime,
NumberCICO
from
dbo.AR_SalespersonLocationTrace
where CAST (UpdateTime as DATE) >= '{datenow_mns1}'
and CAST (UpdateTime as DATE) <= '{datenow_mns1}'
and SUBSTRING(Type,1,2) = 'DE'
)

Select
T1.BranchID,
T1.SlsperID,
Lat,
Lng,
CustID,
Typ,
T1.NumberCICO,
CheckInUpdateTime,
DeOrderNbr,
DE_UpdateTime
from T1
LEFT JOIN T2 ON
T1.NumberCICO = T2.NumberCICO
and T1.BranchID = T2.BranchID
and T1.SlsperID = T1.SlsperID
ORDER BY CheckInUpdateTime ASC
"""

def insert():
    TRACE = get_ms_df(sql)
    TRACE.DeOrderNbr = np.where(TRACE.DeOrderNbr.isna(), None, TRACE.DeOrderNbr)
    TRACE.DE_UpdateTime = pd.to_datetime(TRACE.DE_UpdateTime, dayfirst=True)
    TRACE.DE_UpdateTime.fillna(datetime(1900,1,1), inplace=True)
    execute_values_insert(TRACE, "d_trace_location")
    #BigQuery
    bq_values_insert(TRACE, "d_trace_location", 2)

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

dummy_start >> insert