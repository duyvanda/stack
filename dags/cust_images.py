# %%
from utils.df_handle import *

# %%
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='CUST_IMAGES'
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
}

dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          schedule_interval= '0 0 * * *',
          tags=[prefix+name, 'Sync', 'daily', 'at0']
)

# %%
def update():
    sql = \
    """
    select
    CONCAT(BranchID,CustID,ImageID,ImageFileName) as pk,
    BranchID, 
    CustID,
    ImageID, 
    'https://dms.phanam.com.vn/IMG/'+ImageFileName as image_url, 
    LUpd_DateTime 
    from AR_Customer_BusinessImage
    """
    df=get_ms_df(sql)
    bq_values_insert(df, "d_customer_images", 3)
    print("Done")

# %%
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

update = PythonOperator(task_id="update", python_callable=update, dag=dag)

dummy_start >> update


