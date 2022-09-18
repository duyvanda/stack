# -*- coding: utf-8 -*-
"""pda_sod.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1HiT5K0GHRJm8tiu7w5RJB68WvZ5FK8Sk
"""

from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='PDA_SOD'
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
          schedule_interval= '*/30 8-20,23-23 * * *',
          tags=[prefix+name, 'Sync', '30mins']
)

# from utils.df_handle import *

datenow = datetime.now().strftime("%Y-%m-%d")
datenow_mns1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
fdom = datetime.now().replace(day=1).strftime("%Y%m%d")

from_tb = "OM_PDASalesOrdDet"

usql = \
f"""
DECLARE @from DATE = '{datenow}'
DECLARE @to DATE = '2022-01-31'
SELECT
CONCAT(BranchID,OrderNbr,LineRef) as pk,
BranchID,
OrderNbr,
LineRef,
InvtID,
LineQty,
OrderType,
OrigOrderNbr,
SiteID,
SlsPrice,
Crtd_Prog,
Crtd_User,
Crtd_Datetime,
LUpd_Datetime,
SlsperID,
BeforeVATPrice,
BeforeVATAmount,
AfterVATPrice,
AfterVATAmount,
VATAmount,
FreeItem
from {from_tb}
where cast(LUpd_DateTime as DATE) >= @from
"""

# print(usql)

table_name = "sync_dms_pda_sod"
table_temp = "sync_dms_pda_sod_temp"

#INSERT
def insert():
    print(usql)

def update():
#UPDATE
    df_update = get_ms_df(usql)
    try:
        assert df_update.shape[0] >0,"NO DATA TO INPUT"
        df_update.columns = lower_col(df_update)
        df_update['crtd_datetime1'] = df_update['crtd_datetime'].dt.normalize()
        df_update1 = df_update['crtd_datetime1']
        drop_cols(df_update, "crtd_datetime1")
        df_update1.drop_duplicates(inplace=True)
        tpl_dt = tuple(df_update1.dt.strftime('%Y-%m-%d').to_list()) + ('1900-01-01','1900-01-01')
        # tpl_dt
        df_update1 = df_update['pk']
        df_update1.drop_duplicates(inplace=True)
        tpl_pk = tuple(df_update1.to_list())+ ('','')
        del_sql = \
        f"""
        DELETE FROM biteam.{table_name}
        WHERE
        DATE(crtd_datetime) in {tpl_dt}
        AND pk in {tpl_pk}
        """
        print("del_sql ",del_sql)
        execute_bq_query(del_sql)
        # DELTE CAC DON TAO VA XOA TRONG NGAY, DMS KHONG CO STATUS DELETED
        dsql = \
        f"""
        delete from biteam.{table_name} where date(crtd_datetime) >= '{datenow}'
        """
        print("delete_sql: ", dsql)
        execute_bq_query(dsql)
        #
        df_update['inserted_at'] = datetime.now()
        bq_values_insert(df_update, f"{table_name}", 2)
    except AssertionError:
        print("NO DATA TO INPUT")

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

update = PythonOperator(task_id="update", python_callable=update, dag=dag)

dummy_start >> insert >> update

