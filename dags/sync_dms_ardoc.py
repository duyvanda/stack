from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='ARDOC'
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

from_tb = "AR_Doc"
table_name = "sync_dms_ardoc"
table_temp = "sync_dms_ardoc_temp"

start_date = '2022-01-01'
datenow = datetime.now().strftime("%Y-%m-%d")
datenow_mns1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
fdom = datetime.now().replace(day=1).strftime("%Y%m%d")
datenow_mns90 = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
date_ms45 = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")

usql = \
f"""
DECLARE @from DATE = '{datenow}'
DECLARE @to DATE = '2022-01-01'
SELECT
CONCAT(BranchID,BatNbr,RefNbr) as pk,
BranchID,
BatNbr,
RefNbr,
CustId,
DiscDate,
DocBal,
DocDate,
DocDesc,
DocType,
DueDate,
InvcNbr,
InvcNote,
NoteId,
OrdNbr,
OrigDocAmt,
Rlsed,
SlsperId,
Terms,
LUpd_DateTime,
LUpd_Prog,
LUpd_User,
Crtd_DateTime,
Crtd_Prog,
Crtd_User
from {from_tb}
where cast(LUpd_DateTime as DATE) >= @from
and cast(Crtd_DateTime as DATE) >= @to
"""

def update():
    # pass
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
    select CONCAT(BranchID,BatNbr,RefNbr) as pk from {from_tb} where cast (Crtd_DateTime as date ) >= '{date_ms45}'
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
    dk = datetime.now().hour in {6} and datetime.now().minute < 30
    if dk:
        try:
            status_sql = \
            f"""
            select count(*) as count FROM {from_tb}
            """
            countdms = get_ms_df(status_sql)
            # countdms.head()
            print("countdms", countdms)
            statusbq_sql = \
            f"""
            SELECT count(*) as count from biteam.{table_name}
            """
            countbq = get_bq_df(statusbq_sql)
            print("countbq",countbq)
            dfs = dfs_diff(countbq, countdms)
            assert dfs.shape[0] == 0, "NO DIFF"
            print("NO DIFF")
        except AssertionError:
            sql = \
            f"""
            DECLARE @from DATE = '{datenow_mns90}'
            DECLARE @to DATE = '2022-01-31'
            select
            CONCAT(BranchID,BatNbr,RefNbr) as pk,
            BranchID,
            BatNbr,
            RefNbr,
            CustId,
            DiscDate,
            DocBal,
            DocDate,
            DocDesc,
            DocType,
            DueDate,
            InvcNbr,
            InvcNote,
            NoteId,
            OrdNbr,
            OrigDocAmt,
            Rlsed,
            SlsperId,
            Terms,
            LUpd_DateTime,
            LUpd_Prog,
            LUpd_User,
            Crtd_DateTime,
            Crtd_Prog,
            Crtd_User
            from {from_tb}
            where cast(Crtd_DateTime as DATE) >= @from
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
            print("countbq",countbq)
            ##
    else:
        print("Not a good time", datetime.now().hour)

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

update = PythonOperator(task_id="update", python_callable=update, dag=dag)

update_sync_dms_ardoc = PythonOperator(task_id="update_sync_dms_ardoc", python_callable=update_sync_dms_ardoc, dag=dag)

dummy_start >> update >> insert >> update_sync_dms_ardoc

