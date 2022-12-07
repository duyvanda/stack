from utils.df_handle import *
import pickle
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.providers.tableau.operators.tableau_refresh_workbook import TableauRefreshWorkbookOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='DSKHMOI'
prefix='DMS_'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2021, 10, 1, tzinfo=local_tz),
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
          schedule_interval= '@daily',
          tags=[prefix+name, 'Daily']
)


# day_ago = 6
datenow = datetime.now().strftime("%Y-%m-%d")
# datenow_day_ago = ( datetime.now()-timedelta(day_ago) ).strftime("%Y-%m-%d")
# param_1 = f"'{datenow_day_ago}'"
param_2 = f'2022-02-01'
param_3 = f"'{datenow}'"
# param_4 = f"'20211109'"

def extract_dms():
    query = f"EXEC [PR_AR_PDACustNew_BI] @Fromdate='{param_2}', @Todate={param_3}"
    df = get_ms_df(sql=query)
    df.columns = lower_col(df)
    df.columns = cleancols(df)
    df.to_pickle("df.pk")
    return None


def insert():
    df = pd.read_pickle("df.pk")
    df = df[['slsperid','custid','status','createdate','approvedate','crtd_user']].copy()
    df.columns = ['slsperid', 'custid', 'status', 'crtd_date', 'approve_date','crtd_user']
    df = df[df.approve_date.notna()].copy()
    bq_values_insert(df, "d_manual_dskhmoi", 3)


dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

extract_dms = PythonOperator(task_id="extract_dms", python_callable=extract_dms, dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

dummy_start >> extract_dms >> insert