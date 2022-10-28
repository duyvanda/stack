# %%
from utils.df_handle import *

# %%
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor

local_tz = pendulum.timezone("Asia/Bangkok")

name='sp_data_checkin_pbh'
prefix='TRIGGERBQ_'
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
          schedule_interval= '*/30 8-23 * * *',
          tags=[prefix+name, 'trigger']
)


def call_bq_ps():
    execute_bq_query("call view_mapping.sp_data_checkin_pbh()")

def remove_file():
    os.remove(csv_path+"FILESENSOR/DMS_CheckIn.txt")
    os.remove(csv_path+"FILESENSOR/SYNC_SACHECKIN.txt")
    os.remove(csv_path+"FILESENSOR/SYNC_OC.txt")


# %%

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

checkin = FileSensor(task_id="checkin", filepath=csv_path+"FILESENSOR/DMS_CheckIn.txt", poke_interval=10, dag=dag, timeout=120)

sacheckin = FileSensor(task_id="sacheckin" ,filepath=csv_path+"FILESENSOR/SYNC_SACHECKIN.txt", poke_interval=10, dag=dag, timeout=120)

oc = FileSensor(task_id="oc", filepath=csv_path+"FILESENSOR/SYNC_OC.txt", poke_interval=10, dag=dag, timeout=120)

call_bq_ps = PythonOperator(task_id="call_bq_ps", python_callable=call_bq_ps, dag=dag)

remove_file = PythonOperator(task_id="remove_file", python_callable=remove_file, dag=dag)

dummy_start >> checkin >> sacheckin >> oc >> call_bq_ps >> remove_file


