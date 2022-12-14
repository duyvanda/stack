# -*- coding: utf-8 -*-
"""DeliverLeadTime.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1Gxr_p0uB3pA8DegM1C3Qt1ot74F3ymLe
"""

from utils.df_handle import *

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='DeliverLeadTime'
prefix='DMS_'
csv_path = '/usr/local/airflow/plugins'+'/'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2022, 5, 5, tzinfo=local_tz),
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
          tags=[prefix+name, 'DEAD', 'at 00:05']
)

datenow_mns1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# datenow_mns1 = '2022-04-01'

sql = \
f"""
select
a.BranchID,
a.OrderNbr,
case when d.DeliveryUnit = 'CW' then N'Chành Xe'
when d.DeliveryUnit = 'PN' then N'Pha Nam' end dvvc,
a.SlsperID as manvgh,
[Status] = 'Đã Giao Hàng',
b.CustID,
b.InvcNbr,
b.InvcNote,
e.Remark,
e.Crtd_DateTime as post_time,
e.Crtd_User as post_user,
g.ErrorMessage as pending_reason,
e.LUpd_DateTime as approve_time,
e.LUpd_User as approve_user,
f.LUpd_DateTime as invoice_time,
f.LUpd_User as invoice_user,
a.Crtd_DateTime as booked_time,
a.Crtd_User as booked_user,
ready_to_ship_time = a.Crtd_DateTime,
a.Crtd_User as rts_user,
a.LUpd_DateTime as delivered_time,
a.SlsperID as delivered_user,
datediff(minute, e.Crtd_DateTime, e.LUpd_DateTime) as leadtime_t0_minute,
datediff(minute, e.LUpd_DateTime, f.LUpd_DateTime) as leadtime_t1_minute,
datediff(minute, f.LUpd_DateTime, a.Crtd_DateTime) as leadtime_t2_minute,
datediff(minute, a.Crtd_DateTime, a.Crtd_DateTime) as leadtime_t3_minute,
datediff(minute, a.Crtd_DateTime, a.LUpd_DateTime) as leadtime_t4_minute,
datediff(minute, e.Crtd_DateTime, a.LUpd_DateTime) as leadtime_full_minute

from OM_Delivery a
--split ra theo nhieu sku va hd
LEFT JOIN OM_SalesOrd b ON
a.BranchID = b.BranchID and
a.OrderNbr = b.OrigOrderNbr
LEFT JOIN OM_Issuebookdet c ON
a.BranchID = c.BranchID and
a.OrderNbr = c.OrderNbr
LEFT JOIN OM_Issuebook d ON
c.BranchID = d.BranchID and
c.BatNbr = d.BatNbr
LEFT JOIN OM_PDASalesOrd e ON
a.BranchID = b.BranchID and
a.OrderNbr = e.OrderNbr
LEFT JOIN OM_Invoice f on
b.BranchID = f.BranchID and
b.InvcNbr = f.InvcNbr and
b.InvcNote = f.InvcNote and
b.ARRefNbr = f.RefNbr
LEFT JOIN API_HistoryOM205 g ON
a.BranchID = g.BranchID and
a.OrderNbr = g.OrderNbr
and g.Status = 'E'
where a.Status = 'C'
and Cast(a.LUpd_DateTime as date) >= '{datenow_mns1}'
--and Cast(a.Crtd_DateTime as date) >= '2022-04-01'
and datediff(minute, e.LUpd_DateTime, f.LUpd_DateTime) >= 0
Order By a.LUpd_DateTime DESC
"""

bq_sql = f"delete from `spatial-vision-343005.biteam.f_mds_leadtime` where date(delivered_time) >= '{datenow_mns1}'"

def insert():
    df = get_ms_df(sql)
    # datenow_mns1
    bq_values_insert(df,"f_mds_leadtime",2)

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

dummy_start >> insert