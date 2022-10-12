
from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.tableau.operators.tableau_refresh_workbook import TableauRefreshWorkbookOperator


local_tz = pendulum.timezone("Asia/Bangkok")

name='-dms'
prefix='D_USERS'
csv_path = '/usr/local/airflow/plugins'+'/'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2021, 10, 1, tzinfo=local_tz),
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
          schedule_interval= '*/30 8-17,22-22 * * *',
          tags=[prefix+name, 'Daily', '30mins']
)

def update_table():
    query = \
    """
    SELECT 
    --DISTINCT
    SlsperID as manv,
    u.UserTypes as position,
    u.FirstName as tencvbh,
    SupID,
    sup.FirstName as tenquanlytt,
    ASM,
    asm.FirstName as tenquanlykhuvuc,
    RSMID,
    rsm.FirstName as tenquanlyvung,
    'DMS' as datatype
    FROM dbo.fr_ListSaleByData('admin') as a
    LEFT JOIN dbo.Users u WITH (NOLOCK) ON u.UserName=a.SlsperID
    LEFT JOIN dbo.Users sup WITH (NOLOCK) ON sup.UserName=a.SupID
    LEFT JOIN dbo.Users asm WITH (NOLOCK) ON asm.UserName=a.ASM
    LEFT JOIN dbo.Users Rsm WITH (NOLOCK) ON rsm.UserName =a.RSMID
    --where SlsperID = 'MR1953'
    where RSMID is not null

    UNION ALL

    SELECT 
    --DISTINCT
    SlsperID as manv,
    u.UserTypes as position,
    u.FirstName as tencvbh,
    SupID,
    sup.FirstName as tenquanlytt,
    ASM,
    asm.FirstName as tenquanlykhuvuc,
    RSMID,
    rsm.FirstName as tenquanlyvung,
    'DMS' as datatype
    FROM dbo.fr_ListSaleByData('admin') as a
    LEFT JOIN dbo.Users u WITH (NOLOCK) ON u.UserName=a.SlsperID
    LEFT JOIN dbo.Users sup WITH (NOLOCK) ON sup.UserName=a.SupID
    LEFT JOIN dbo.Users asm WITH (NOLOCK) ON asm.UserName=a.ASM
    LEFT JOIN dbo.Users Rsm WITH (NOLOCK) ON rsm.UserName =a.RSMID
    --where SlsperID = 'MR1953'
    where RSMID is null
    """

    print("query is ", query)

    df1 = get_ms_df(query)
    df1['tenquanlykhuvuc'].replace("Dương Tấn Bửu", "Dương Tấn Bửu(KN)", inplace=True)
    df1['tenquanlykhuvuc'].replace("Nguyễn Thọ Chiến", "Nguyễn Thọ Chiến(KN)", inplace=True)
    df1['tenquanlykhuvuc'].replace("Bùi Hữu Toàn", "Bùi Hữu Toàn(KN)", inplace=True)
    df1['tenquanlykhuvuc'].replace("Nguyễn Hoàng Viển", "Nguyễn Hoàng Viển(KN)", inplace=True)
    df1['tenquanlykhuvuc'].replace("Võ Thị Kim Liên", "Nguyễn Thọ Chiến(KN)", inplace=True)
    # df_users = get_ps_df("select manv from d_users")
    if True:
        df1['tenquanlytt'].fillna(df1.tenquanlykhuvuc, inplace=True)
        df1['tenquanlykhuvuc'].fillna(df1.tenquanlyvung, inplace=True)
        #13/01
        df1.drop_duplicates(keep='first', inplace=True, subset=['manv'])
        bq_values_insert(df1, 'd_users', 3)
        commit_psql("truncate table d_users cascade;")
        drop_cols(df1, ['supid', 'asm', 'rsmid'])
        execute_values_insert(df1, "d_users")
    else:
        None


dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

# py_get_csv_files = PythonOperator(task_id="get_csv_files", python_callable=get_csv_files, dag=dag)

py_update_table = PythonOperator(task_id="update_table", python_callable=update_table, dag=dag)


# hello_task4 = ToCSVMsSqlOperator(task_id='sample-task-4', mssql_conn_id="1_dms_conn_id", sql=sql, database="PhaNam_eSales_PRO", path=path, dag=dag)

# tab_refresh = TableauRefreshWorkbookOperator(task_id='tab_refresh', workbook_name='Báo Cáo Doanh Thu Tiền Mặt', dag=dag)


dummy_start >> py_update_table
# >> tab_refresh
