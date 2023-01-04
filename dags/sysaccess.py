from utils.df_handle import *

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.tableau.operators.tableau_refresh_workbook import TableauRefreshWorkbookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


local_tz = pendulum.timezone("Asia/Bangkok")

name='SysAccess'
prefix='DMS_'
# csv_path = '/usr/local/airflow/plugins'+'/'
# path = '/usr/local/airflow/dags/files/csv_congno/'
# pk_path = '/usr/local/airflow/plugins/Debt_DataDoanhThuPickles/'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2021, 10, 1, tzinfo=local_tz),
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    # 'email':['duyvq@merapgroup.com', 'vanquangduy10@gmail.com'],
    'do_xcom_push': False,
    'execution_timeout':timedelta(seconds=600)
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=10),
}

dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          schedule_interval= '55 23 * * *',
          tags=[prefix+name, 'Daily']
)

def insert_access():
    m = datetime.now().month
    nm = m+1 if m != 12 else 1
    y = datetime.now().year
    ny = datetime.now().year if m != 12 else y+1
    # str(m) + str(y)
    fdom = datetime(y, m, 1)
    ldom = datetime(ny, nm, 1) - timedelta(days=1)
    fdom = fdom.strftime("%Y-%m-%d")
    ldom = ldom.strftime("%Y-%m-%d")
    my = str(m) + str(y)

    sql = \
    f"""
    SELECT a.AccessDate, u.UserName,u.FirstName,sc.ScreenNumber,sc.Descr,a.SessionNumber, a.InternetAddress, sc.Screentype as ComputerName, a.CompanyID
    FROM dbo.SYS_Access a
    INNER JOIN dbo.Users u ON a.UserId=u.UserName
    and u.UserTypes like '%CS%'
    INNER JOIN dbo.SYS_Screen sc ON sc.ScreenNumber=a.ScreenNumber
    WHERE CAST(a.AccessDate AS DATE) BETWEEN '{fdom}' AND '{ldom}'
    """
    print(sql)
    df = get_ms_df(sql)
    df['my'] = my
    execute_bq_query(f""" DELETE FROM `spatial-vision-343005.biteam.d_sync_sysaccess` where my = '{my}' """)
    bq_values_insert(df, "d_sync_sysaccess", 2)


def insert_rptrunning():
    rp_sql = \
    """
    SELECT [Mã Báo Cáo]=r.ReportNbr,[Tên Báo Cáo]=ISNULL(e.Name,c.Descr),r.UserID,[Tên Người Dùng]=u.FirstName,u.Position, u.UserTypes,[Ngày Xem Báo Cáo]=r.ReportDate
    , [(Param)String_1]=CASE WHEN expa.StringCap00<>'' THEN CASE WHEN r.StringParm00='' THEN dbo.fr_GetLang(1,expa.StringCap00) +':'+'ALL' ELSE dbo.fr_GetLang(1,expa.StringCap00)+':'+r.StringParm00 end ELSE CASE WHEN cp.StringCap00 <> '' THEN CASE WHEN r.StringParm00='' THEN dbo.fr_GetLang(1,cp.StringCap00)+':'+'ALL' ELSE dbo.fr_GetLang(1,cp.StringCap00)+':'+r.StringParm00 end ELSE '' END end 
    , [(Param)String_2]=CASE WHEN expa.StringCap01<>'' THEN CASE WHEN r.StringParm01='' THEN dbo.fr_GetLang(1,expa.StringCap01)+':'+'ALL' ELSE dbo.fr_GetLang(1,expa.StringCap01)+':'+r.StringParm01 END  ELSE CASE WHEN cp.StringCap01 <> '' THEN CASE WHEN  r.StringParm01='' THEN dbo.fr_GetLang(1,cp.StringCap01)+':'+'ALL' ELSE dbo.fr_GetLang(1,cp.StringCap01)+':'+r.StringParm01 end ELSE '' END end 
    , [(Param)String_3]=CASE WHEN expa.StringCap02<>'' THEN CASE WHEN r.StringParm02='' THEN dbo.fr_GetLang(1,expa.StringCap02)+':'+'ALL' ELSE dbo.fr_GetLang(1,expa.StringCap02)+':'+r.StringParm02 END  ELSE CASE WHEN cp.StringCap02 <> '' THEN CASE WHEN  r.StringParm02='' THEN dbo.fr_GetLang(1,cp.StringCap02)+':'+'ALL' ELSE dbo.fr_GetLang(1,cp.StringCap02)+':'+r.StringParm02 end ELSE '' END end 
    , [(Param)String_4]=CASE WHEN expa.StringCap03<>'' THEN CASE WHEN r.StringParm03='' THEN dbo.fr_GetLang(1,expa.StringCap03)+':'+'ALL' ELSE dbo.fr_GetLang(1,expa.StringCap03)+':'+r.StringParm03 END ELSE CASE WHEN cp.StringCap03 <> '' THEN CASE WHEN  r.StringParm03='' THEN dbo.fr_GetLang(1,cp.StringCap03)+':'+'ALL' ELSE dbo.fr_GetLang(1,cp.StringCap03)+':'+r.StringParm03 end ELSE '' END end 
    ,[(Param)Date_1]=CASE WHEN expa.DateCap00<>'' THEN dbo.fr_GetLang(1,expa.DateCap00)+':'+CONVERT(VARCHAR,r.DateParm00, 103)  ELSE CASE WHEN cp.DateCap00 <> '' THEN  dbo.fr_GetLang(1,cp.DateCap00)+':'+CONVERT(VARCHAR,r.DateParm00, 103) ELSE '' END end 
    ,[(Param)Date_2]=CASE WHEN expa.DateCap01<>'' THEN dbo.fr_GetLang(1,expa.DateCap01)+':'+CONVERT(VARCHAR,r.DateParm01, 103)  ELSE CASE WHEN cp.DateCap01 <> '' THEN dbo.fr_GetLang(1,cp.DateCap01)+':'+CONVERT(VARCHAR,r.DateParm01, 103) ELSE '' END end 
    ,[(Param)Date_3]=CASE WHEN expa.DateCap02<>'' THEN dbo.fr_GetLang(1,expa.DateCap02)+':'+CONVERT(VARCHAR,r.DateParm02, 103)  ELSE CASE WHEN cp.DateCap02 <> '' THEN  dbo.fr_GetLang(1,cp.DateCap02)+':'+CONVERT(VARCHAR,r.DateParm02, 103) ELSE '' END end 
    ,[(Param)Date_4]=CASE WHEN expa.DateCap03<>'' THEN dbo.fr_GetLang(1,expa.DateCap03)+':'+CONVERT(VARCHAR,r.DateParm03, 103)  ELSE CASE WHEN cp.DateCap03 <> '' THEN  dbo.fr_GetLang(1,cp.DateCap03)+':'+CONVERT(VARCHAR,r.DateParm03, 103) ELSE '' END end 
    
    FROM dbo.RPTRunning r WITH (NOLOCK)
    INNER JOIN dbo.Users u WITH (NOLOCK) ON r.UserID=u.UserName
    LEFT JOIN SYS_ReportExport e WITH (NOLOCK) ON e.ReportNbr = r.ReportNbr
    LEFT JOIN dbo.SYS_ReportExportParm expa WITH (NOLOCK) ON expa.ReportNbr=e.ReportNbr
    LEFT JOIN dbo.SYS_ReportControl c WITH (NOLOCK) ON c.ReportNbr=r.ReportNbr
    LEFT JOIN dbo.SYS_ReportParm cp WITH (NOLOCK) ON cp.ReportNbr=c.ReportNbr
    WHERE r.ReportNbr NOT IN ('OM20890','OM45400','OM32700','IN11500','AR10200','AR10100')
    """
    # df['inserted_at'] = datetime.now()
    df = get_ms_df(rp_sql)
    bq_values_insert(df, "d_sync_rptrunning", 2)

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

insert_access = PythonOperator(task_id="insert_access", python_callable=insert_access, dag=dag)

insert_rptrunning = PythonOperator(task_id="insert_rptrunning", python_callable=insert_rptrunning, dag=dag)

dummy_start >> insert_access >> insert_rptrunning
