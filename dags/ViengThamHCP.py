from utils.df_handle import *

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.tableau.operators.tableau_refresh_workbook import TableauRefreshWorkbookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


local_tz = pendulum.timezone("Asia/Bangkok")

name='WorkPlan'
prefix='ETC_'
csv_path = '/usr/local/airflow/plugins'+'/'
path = '/usr/local/airflow/dags/files/csv_congno/'
pk_path = '/usr/local/airflow/plugins/Debt_DataDoanhThuPickles/'

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
          schedule_interval= '@daily',
          tags=[prefix+name, 'Daily']
)

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

def extract_dms():
    ETC_WorkingResult = get_ms_df(f"""exec [pr_ETC_WorkingResult_BI] '{fdom}', '{ldom}'""")
    ETC_WorkingPlan = get_ms_df(f"""exec [pr_ETC_WorkingPlan_BI] '{fdom}', '{ldom}' """)
    ETC_WorkingResult.to_pickle("ETC_WorkingResult.pk")
    ETC_WorkingPlan.to_pickle("ETC_WorkingPlan.pk")


def insert():
    ETC_WorkingResult = pd.read_pickle("ETC_WorkingResult.pk")
    ETC_WorkingPlan = pd.read_pickle("ETC_WorkingPlan.pk")

    ETC_WorkingResult.VisitDate =  pd.to_datetime(ETC_WorkingResult.VisitDate)
    ETC_WorkingResult['my'] = my
    ETC_WorkingResult['inserted_at'] = datetime.now()
    execute_bq_query(f""" DELETE FROM `spatial-vision-343005.biteam.sync_dms_etc_workingresult` where my = '{my}' """)
    bq_values_insert(ETC_WorkingResult, "sync_dms_etc_workingresult", 2)
    
    ETC_WorkingPlan['my'] = str(m) + str(y)
    ETC_WorkingPlan['inserted_at'] = datetime.now()
    execute_bq_query(f""" DELETE FROM `spatial-vision-343005.biteam.sync_dms_etc_workingplan` where my = '{my}' """)
    bq_values_insert(ETC_WorkingPlan, "sync_dms_etc_workingplan", 2)


dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

extract_dms = PythonOperator(task_id="extract_dms", python_callable=extract_dms, dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

dummy_start >> extract_dms >> insert