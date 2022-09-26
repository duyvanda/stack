from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='DELIHISTORY'
prefix='SYNC_'
csv_path = '/usr/local/airflow/plugins'+'/'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2022, 5, 10, tzinfo=local_tz),
    'do_xcom_push': False,
    'execution_timeout':timedelta(seconds=300)
}

dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          schedule_interval= '*/30 6-23 * * *',
          tags=[prefix+name, 'Sync', '30mins']
)
from_tb = "OM_DeliHistory"
from_tb2 = "OM_DeliHistory"
table_name = "sync_dms_delihistory"
table_temp = "sync_dms_delihistory_temp"
start_date = '2022-01-01'
datenow = datetime.now().strftime("%Y-%m-%d")
datenow_mns1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
fdom = datetime.now().replace(day=1).strftime("%Y%m%d")
datenow_mns90 = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
date_ms45 = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")

usql = \
f"""
DECLARE @from DATE = '{datenow}';
DECLARE @to DATE = '2022-01-01';
SELECT
REPLACE(CONCAT(BranchID, BatNbr,'-',OrderNbr,'-', CONVERT(varchar, ShipDate, 12),CONVERT(varchar, ShipDate, 24)),':','') as pk,
BranchID,
BatNbr,
OrderNbr,
SlsperID,
Reason,
[Status] as status,
a.Crtd_DateTime,
a.LUpd_DateTime,
b.Descr,
b.Note,
GETDATE() as inserted_at
from OM_DeliHistory a
LEFT JOIN OM_ReasonCodePPC b
on a.Reason =  b.Code
and b.Code <> '' and b.Type in('DELIOUTLET','DELIREJ','RENOTDEBT')
WHERE a.Status in ('A','D')
and cast(LUpd_DateTime as DATE) >= @from
and cast(a.Crtd_DateTime as DATE) >= @to
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

def handle_deleted_data():
    dmssql = \
    f"""
    SELECT
    REPLACE(CONCAT(BranchID, BatNbr,'-',OrderNbr,'-', CONVERT(varchar, ShipDate, 12),CONVERT(varchar, ShipDate, 24)),':','') as pk
    from {from_tb} 
    where 
    cast(Crtd_DateTime as date ) >= '{date_ms45}'
    and Status in ('A','D')
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

def _update_sync_dms():
    print("_update_sync_dms")

def update_sync_dms():
    _update_sync_dms()
    print("update_sync_dms_2")

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

handle_deleted_data = PythonOperator(task_id="handle_deleted_data", python_callable=handle_deleted_data, dag=dag)

update_sync_dms = PythonOperator(task_id="update_sync_dms", python_callable=update_sync_dms, dag=dag)

dummy_start >> update >> handle_deleted_data >> update_sync_dms

