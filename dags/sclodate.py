# -*- coding: utf-8 -*-
"""SCLoDate.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1IemuXFPBjRqCfk3hCorEG4i0b8PZOgI0
"""

from utils.df_handle import *

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")
name='LoDate'
prefix='SC_'
path = f'/usr/local/airflow/plugins/{prefix}{name}/'

# datenow_min1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2022, 4, 14, tzinfo=local_tz),
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
          schedule_interval= '0 1 * * *',
          tags=[prefix+name, 'Sync', 'at0']
)

sql = \
"""
SELECT
d.BranchID as BranchID,
d.InvtID,
SiteID=d.SiteID,
s.Name,
d.TranDate,
d.Crtd_DateTime,
--d.JrnlType,
--d.TranType,
--T.TranType as ltTranType,
LotSerNbr=isnull(t.LotSerNbr,'')
,ExpDate=isnull(t.ExpDate,''),
Qty = d.CnvFact * d.InvtMult * isnull(T.Qty,d.Qty)
--NhapKhac=0,
--QuyCach=dbo.fr_OM_GetCnvFact(d.InvtID)
FROM dbo.IN_Trans d WITH(NOLOCK)                   
INNER JOIN dbo.IN_LotTrans T WITH(NOLOCK) ON T.BranchID = D.BranchID AND t.BatNbr = d.BatNbr  AND t.RefNbr = d.RefNbr AND t.INTranLineRef = d.LineRef AND t.InvtID = d.InvtID
INNER JOIN dbo.IN_Site s ON d.SiteID = s.SiteId
--WHERE CAST(d.TranDate AS DATE) BETWEEN CAST('20210501' AS DATE) AND CAST('20220627' AS DATE)
AND d.JrnlType  IN ('PO')
AND d.TranType IN ('RC') AND t.Qty >0          
AND d.Rlsed  =  1       
AND d.InvtMult>0
--and t.LotSerNbr = '0020522'
--and d.BranchID = 'MR0001'
"""

def insert():
    print(sql)

def update():
    dms_users = get_ms_df(sql)
    bq_values_insert(dms_users, "d_sc_lodate", 3)


dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

update = PythonOperator(task_id="update", python_callable=update, dag=dag)

dummy_start >> insert >> update