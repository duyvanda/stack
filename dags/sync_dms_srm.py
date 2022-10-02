from utils.df_handle import *

# from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='SRM'
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
    'execution_timeout':timedelta(seconds=300),
    'retries': 3 ,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          schedule_interval= '0 2 * * *',
          tags=[prefix+name, 'Sync', 'Daily']
)

from_tb1 = "OM_SalesRouteMaster"
from_tb2 = "OM_SalespersonRoute"
from_tb3 = "OM_SalesRoute"
from_tb4 = "OM_BranchRoute"
table_name = "sync_dms_srm"
table_temp = "sync_dms_srm_temp"

sql = \
f"""
select
a.SalesRouteID,
a.CustID,
a.DelRouteDet,
a.SlsFreq,
a.WeekofVisit,
concat('MS',a.Mon,a.Tue,a.Wed,a.Thu,a.Fri,a.Sat,a.Sun) as Weekdate,
a.Crtd_DateTime,
a.StartDate,
a.EndDate,
a.SubRouteID,
a.Crtd_User,
1 as split,
b.SlsperID,
b.BranchID,
1 as split2,
c.Descr as SRDescr,
c.BranchID as BranchRouteID,
c.RouteType,
c.Active,
d.Descr as BDescr
from {from_tb1} a
LEFT JOIN {from_tb2} b
ON a.SalesRouteID =  b.SalesRouteID
LEFT JOIN {from_tb3} c
ON a.SalesRouteID =  c.SalesRouteID
LEFT JOIN {from_tb4} d
ON c.BranchID =  d.BranchRouteID
"""

def insert():
    print(sql)

def update():
    df = get_ms_df(sql)
    df['inserted_at'] = datetime.now()
    try:
        print("data shape", df.shape)
        assert df.shape[0] >0
    except AssertionError:
        print("No customer changed")
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
