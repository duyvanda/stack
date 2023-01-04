from utils.df_handle import *

# from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='BATCH'
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
          schedule_interval= '*/30 6-23 * * *',
          tags=[prefix+name, 'Sync', '30mins']
)

from_tb = "Batch"
# from_tb2 = "OM_DeliReportDet"
table_name = "sync_dms_batch"
table_temp = "sync_dms_batch_temp"

start_date = '2022-01-01'
datenow = datetime.now().strftime("%Y-%m-%d")
datenow_mns1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
fdom = datetime.now().replace(day=1).strftime("%Y%m%d")
datenow_mns90 = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
date_ms45 = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")

usql = \
f"""
DECLARE @from DATE = '{datenow}'
DECLARE @to DATE = '2022-01-01'
SELECT
CONCAT(BranchID,Module,BatNbr) as pk,
BranchID, 
Module,
BatNbr,
RefNbr,
JrnlType,
Descr,
TotAmt,
Status,
Rlsed,
Crtd_DateTime,
Crtd_User,
LUpd_DateTime,
LUpd_User
from {from_tb}
where Module = 'AR'
and cast(LUpd_DateTime as DATE) >= @from
and cast(Crtd_DateTime as DATE) >= @to
"""

def update():
    try:
        #UPDATE
        df_update = get_ms_df(usql)
        assert df_update.shape[0] >0,"NO DATA TO INPUT"
        df_update.columns = lower_col(df_update)
        # df_update.columns
        df_update['crtd_datetime1'] = df_update['crtd_datetime'].dt.normalize()
        df_update1 = df_update['crtd_datetime1']
        drop_cols(df_update, "crtd_datetime1")
        df_update1.drop_duplicates(inplace=True)
        tpl_dt = tuple(df_update1.dt.strftime('%Y-%m-%d').to_list()) + ('1900-01-01','1900-01-01')
        # tpl_dt
        df_update1 = df_update['pk']
        df_update1.drop_duplicates(inplace=True)
        tpl_pk = tuple(df_update1.to_list()) + ('','')
        del_sql = \
        f"""
        DELETE FROM biteam.{table_name} WHERE DATE(crtd_datetime) in {tpl_dt} AND pk in {tpl_pk}
        """
        print("del_sql ",del_sql)
        execute_bq_query(del_sql)
        df_update['inserted_at'] = datetime.now()
        bq_values_insert(df_update, f"{table_name}", 2)
    except AssertionError:
        print("NO DATA TO INPUT")

def insert():
    dmssql = \
    f"""
    select CONCAT(BranchID,Module,BatNbr) as pk from {from_tb} where cast (Crtd_DateTime as date ) >= '{date_ms45}'
    """
    dfdms =  get_ms_df(dmssql)
    a = set(dfdms.pk.to_list())
    bqssql = \
    f"""
    select pk from biteam.{table_name} where date(crtd_datetime) >= '{date_ms45}'
    """
    dfbq = get_bq_df(bqssql)
    b = set(dfbq.pk.to_list())
    del_tp = tuple(b.difference(a)) + ('','')
    print("del tuple ",del_tp)
    bqsql = \
    f"""
    delete from biteam.{table_name} where date(crtd_datetime) >= '{date_ms45}' and pk in {del_tp}
    """
    print(bqsql)
    execute_bq_query(bqsql)

def update_sync_dms_ardoc():
    try:
        status_sql = \
        f"""
        select count(*) as count FROM {from_tb} where Module = 'AR'
        """
        countdms = get_ms_df(status_sql)
        # countdms.head()
        print(countdms)
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name}
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        dfs = dfs_diff(countbq, countdms)
        assert False, "NO DIFF"
        print("NO DIFF")
    except AssertionError:
        sql = \
        f"""
        DECLARE @from DATE = '{datenow_mns90}'
        DECLARE @to DATE = '2022-01-31'
        SELECT
        CONCAT(BranchID,Module,BatNbr) as pk,
        BranchID, 
        Module,
        BatNbr,
        RefNbr,
        JrnlType,
        Descr,
        TotAmt,
        Status,
        Rlsed,
        Crtd_DateTime,
        Crtd_User,
        LUpd_DateTime,
        LUpd_User
        from {from_tb}
        where cast(Crtd_DateTime as DATE) >= @from and Module = 'AR'
        """
        df = get_ms_df(sql)
        df['inserted_at'] = datetime.now()

        sql = \
        f"""
        delete from biteam.{table_name} where date(crtd_datetime) >= '{datenow_mns90}'
        """
        print("delete_sql: ", sql)
        execute_bq_query(sql)
        bq_values_insert(df, f"{table_name}", 2)

        #recheck count *
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name}
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        ##

def update_sync_dms_ardoc_2():
    dk = datetime.now().hour in {6} and datetime.now().minute < 30
    if dk: update_sync_dms_ardoc()
    else: print("Not a good time", datetime.now().hour)

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

dummy_end = DummyOperator(task_id="dummy_end", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

update = PythonOperator(task_id="update", python_callable=update, dag=dag)

update_sync_dms_ardoc_2 = PythonOperator(task_id="update_sync_dms_ardoc_2", python_callable=update_sync_dms_ardoc_2, dag=dag)

dummy_start >> update >> insert >> update_sync_dms_ardoc_2 >> dummy_end

# print(table_name, table_temp, datenow_mns45)

# print(datenow_mns45)

# bq_values_insert(df, f"{table_temp}", 3)
# sql = \
# f"""
# DROP TABLE IF EXISTS biteam.{table_name};
# CREATE TABLE biteam.{table_name} LIKE biteam.{table_temp}
# PARTITION BY DATE(crtd_datetime)
# CLUSTER BY branchid,batnbr,refnbr
# """
# execute_bq_query(sql)

# sql = \
# f"""
# delete from biteam.{table_name} where date(crtd_datetime) >= '{datenow_mns45}'
# """
# print("delete_sql: ", sql)
# execute_bq_query(sql)

# bq_values_insert(df, f"{table_name}", 2)

# sql = \
# f"""
# DROP TABLE IF EXISTS biteam.{table_name};
# CREATE TABLE biteam.{table_name} LIKE biteam.{table_temp}
# CLUSTER BY branchid,reportid,batnbr,ordernbr
# """
# execute_bq_query(sql)

# print(sql)

# execute_bq_query("""truncate table `spatial-vision-343005.biteam.sync_dms_dr`""")

# bq_values_insert(df, f"{table_name}", 2)

