from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='CheckIn'
prefix='DMS_'
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
          schedule_interval= '*/30 6-23 * * *',
          tags=[prefix+name, '30mins','Sync', 'Daily']
)

# start_date = '2022-01-01'
datenow = datetime.now().strftime("%Y-%m-%d")
# datenow_mns1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
# fdom = datetime.now().replace(day=1).strftime("%Y-%m-%d")
datenow_mns45 = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")
x_date = datenow if datetime.now().hour in {7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23} else datenow_mns45

def insert():

    sql = \
    f"""
    DECLARE @from DATE = '{x_date}'
    DECLARE @to DATE = '{datenow}'

    SELECT
    BranchID,
    SlsperID,
    Lat,
    Lng,
    CustID,
    Type as Typ,
    CheckType = 'IO',
    NumberCICO,
    UpdateTime
    from dbo.AR_SalespersonLocationTrace 
    where CAST (UpdateTime as DATE) >= @from
    --and CAST (UpdateTime as DATE) <= @to
    and [Type] = 'IO'

    UNION ALL

    SELECT
    BranchID,
    SlsperID,
    Lat,
    Lng,
    CustID,
    Type as Typ,
    CheckType = 'OO',
    NumberCICO,
    UpdateTime
    from dbo.AR_SalespersonLocationTrace 
    where CAST (UpdateTime as DATE) >= @from
    --and CAST (UpdateTime as DATE) <= @to
    and [Type] = 'OO'
    """

    print("sql ",sql)
    TRACE = get_ms_df(sql)
    try:
        assert TRACE.shape[0] > 0
        bqsql = \
        f"""
        delete from biteam.d_checkin where date(updatetime) >= '{x_date}'
        """
        print(bqsql)
        execute_bq_query(bqsql)
        bq_values_insert(TRACE, "d_checkin", 2)

        # create file sensor
        with open(csv_path+f'FILESENSOR/{prefix+name}.txt','w') as f:
            f.close()


    except AssertionError:
        print("There is no data")

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

dummy_end = DummyOperator(task_id="dummy_end", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

dummy_start >> insert >> dummy_end
