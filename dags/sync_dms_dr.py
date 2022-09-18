from utils.df_handle import *

# from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='DR'
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
          schedule_interval= '0 2 * * *',
          tags=[prefix+name, 'Sync', 'Daily']
)

from_tb1 = "OM_DeliReport"
from_tb2 = "OM_DeliReportDet"
table_name = "sync_dms_dr"
table_temp = "sync_dms_dr_temp"

start_date = '2016-01-01'
datenow = datetime.now().strftime("%Y-%m-%d")
datenow_mns1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
fdom = datetime.now().replace(day=1).strftime("%Y-%m-%d")

sql = \
f"""
DECLARE @from DATE = '2016-01-01'
--DECLARE @to DATE = '2022-06-22'

select
a.BranchID,
a.ReportID,
a.Crtd_DateTime,
a.Crtd_Prog,
a.Crtd_User,
a.LUpd_DateTime,
a.LUpd_User,
a.SlsperID,
a.DeliveryUnit,
a.TruckID,
a.ExpectedDate,
a.Package,
a.Ticket,
a.Status,
a.Bridge,
a.Shipcharge,
split = 1,
b.BatNbr,
b.OrderNbr,
b.CustID
from {from_tb1} a
INNER JOIN dbo.{from_tb2} b WITH(NOLOCK) ON b.ReportID = a.ReportID and a.BranchID = b.BranchID
where a.Crtd_DateTime >= @from
"""

def insert():
    print(sql)

def update():
    # pass
    df = get_ms_df(sql)
    df['inserted_at'] = datetime.now()
    execute_bq_query("""truncate table `spatial-vision-343005.biteam.sync_dms_dr`""")
    bq_values_insert(df, f"{table_name}", 2)


dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

update = PythonOperator(task_id="update", python_callable=update, dag=dag)

dummy_start >> insert >> update