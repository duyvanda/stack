from utils.df_handle import *

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.tableau.operators.tableau_refresh_workbook import TableauRefreshWorkbookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


local_tz = pendulum.timezone("Asia/Bangkok")

name='SysAccess'
prefix='DMS_'
# csv_path = '/usr/local/airflow/plugins'+'/'
# path = '/usr/local/airflow/dags/files/csv_congno/'
# pk_path = '/usr/local/airflow/plugins/Debt_DataDoanhThuPickles/'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2021, 10, 1, tzinfo=local_tz),
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    # 'email':['duyvq@merapgroup.com', 'vanquangduy10@gmail.com'],
    'do_xcom_push': False,
    'execution_timeout':timedelta(seconds=600)
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=10),
}

dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          schedule_interval= '55 11 * * *',
          tags=[prefix+name, 'Daily']
)

def insert_access():
    m = datetime.now().month
    nm = m+1 if m != 12 else 1
    y = datetime.now().year
    ny = datetime.now().year if m != 12 else y+1
    # str(m) + str(y)
    fdom = datetime(y, m, 1)
    ldom = datetime(ny, nm, 1) - timedelta(days=1)
    fdom = fdom.strftime("%Y-%m-%d")
    ldom = ldom.strftime("%Y-%m-%d")
    my = str(m) + str(y)

    sql = \
    f"""
    SELECT a.AccessDate, u.UserName,u.FirstName,sc.ScreenNumber,sc.Descr,a.SessionNumber, a.InternetAddress, a.ComputerName, a.CompanyID
    FROM dbo.SYS_Access a
    INNER JOIN dbo.Users u ON a.UserId=u.UserName
    and u.UserTypes like '%CS%'
    INNER JOIN dbo.SYS_Screen sc ON sc.ScreenNumber=a.ScreenNumber
    WHERE CAST(a.AccessDate AS DATE) BETWEEN '{fdom}' AND '{ldom}'
    """
    print(sql)
    df = get_ms_df(sql)
    df['my'] = my
    execute_bq_query(f""" DELETE FROM `spatial-vision-343005.biteam.d_sync_sysaccess` where my = '{my}' """)
    bq_values_insert(df, "d_sync_sysaccess", 2)


dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

insert_access = PythonOperator(task_id="insert_access", python_callable=insert_access, dag=dag)

dummy_start >> insert_access