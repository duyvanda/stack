from utils.df_handle import *

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")
name='MASTER_USER'
prefix='DMS_'
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
    select UserName, FirstName, Position, UserTypes,
    Active = CASE WHEN status='AC' then 1 else 0 end,
    Role = CASE
                WHEN u.UserTypes LIKE '%LOG%' THEN
                    'LOG'
                WHEN u.Position IN ( 'D', 'SD', 'AD', 'RD' )
                    AND u.UserTypes NOT LIKE '%LOG%' THEN
                    'MDS'
                WHEN u.UserTypes LIKE '%CS%' THEN
                    'CS'
                WHEN u.Position IN ( 'S', 'SS', 'AM', 'RM' ) THEN
                    'P.BH'
                ELSE
                    u.Position
            END
    from Users u
    """

def insert():
    print(sql)

def update():
    dms_users = get_ms_df(sql)
    bq_values_insert(dms_users, "d_dms_master_users", 3)


dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

update = PythonOperator(task_id="update", python_callable=update, dag=dag)

dummy_start >> insert >> update