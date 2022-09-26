from utils.df_handle import *

# from os import getcwd
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='SALESROUTEDET'
prefix='SYNC_'
csv_path = '/usr/local/airflow/plugins'+'/'+f'{prefix}{name}/'
# csv_path = getcwd() + "\\"
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
          schedule_interval= '0 6-23 * * *',
          tags=[prefix+name, 'Sync', '60mins']
)

# from_tb = "AR_AdjustDetail"
# from_tb2 = "OM_DeliReportDet"
table_name = "sync_dms_salesroutedet"
table_temp = "sync_dms_salesroutedet_temp"

# df = get_ms_df(sql)
# df['inserted_at'] = datetime.now()
# bq_values_insert(df, f"{table_temp}", 3)

# sql = \
# f"""
# DROP TABLE IF EXISTS biteam.{table_name};
# CREATE TABLE biteam.{table_name} LIKE biteam.{table_temp}
# PARTITION BY DATE(visitdate)
# CLUSTER BY branchid,slsperid,salesrouteid
# """
# execute_bq_query(sql)

# df = get_ms_df(sql)
# df.shape
# bq_values_insert(df, f"{table_name}", 2)

# datetime.now().day

# csv_path = os.getcwd() + "\\"

def update():
    datetime.now().month
    datetime.now().year
    df = pd.read_csv(csv_path+"datetime1.csv")
    # df.dtypes
    df['m'] = datetime.now().month
    df['y'] = datetime.now().year
    dk1 = df.month == df.m
    dk2= df.year == df.y
    df = df[dk1&dk2]
    df.fdom.to_list()

    print("OKIE")

    fdom = df.fdom.to_list()[0]
    ldom = df.ldom.to_list()[0]
    # fdom

    # fdom

    sql = \
    f"""
    DECLARE @from DATE = '{fdom}'
    DECLARE @to DATE = '{ldom}'
    SELECT
    DISTINCT
    REPLACE(CONCAT(vsr.BranchID, vsr.SlsperID,srd.SalesRouteID,srd.CustID,CONVERT(varchar, srd.VisitDate, 12)),':','') as pk,
    vsr.BranchID,
    vsr.SlsperID,
    srd.SalesRouteID,
    sr.Descr,
    srd.CustID,
    srd.VisitDate,
    srd.DayofWeek,
    srd.SlsFreq,
    srd.SlsFreqType,
    srd.WeekofVisit,
    srd.WeekNbr,
    srd.Crtd_Datetime,
    srd.Crtd_Prog,
    srd.Crtd_User,
    srd.LUpd_Datetime,
    srd.LUpd_Prog,
    srd.LUpd_User,
    slr.DelRouteDet,
    inserted_at = getdate()
    from OM_SalesRouteDet srd
    INNER JOIN OM_SalesRoute sr ON
    srd.SalesRouteID = sr.SalesRouteID and
    sr.RouteType in ('B','C','D')
    INNER JOIN dbo.Vs_SalespersonRoute vsr WITH (NOLOCK)
    ON srd.SalesRouteID = vsr.SalesRouteID
    AND srd.VisitDate BETWEEN vsr.FromDate AND vsr.ToDate
    INNER JOIN dbo.vs_OM_SalesRouteMaster slr WITH (NOLOCK)
    ON slr.CustID = srd.CustID
    AND slr.SalesRouteID = srd.SalesRouteID
    AND slr.DelRouteDet = 0
    AND slr.SlsFreq = srd.SlsFreq
    AND srd.VisitDate BETWEEN slr.StartDate AND slr.EndDate
    where cast (srd.VisitDate as date) >= @from and cast (srd.VisitDate as date) <= @to
    """

    print("sql is", sql)

    df = get_ms_df(sql)

    sql = \
    f"""
    delete from biteam.{table_name} where date(visitdate) >= '{fdom}' and date(visitdate) <= '{ldom}' 
    """
    print("delete_sql: ", sql)
    execute_bq_query(sql)
    bq_values_insert(df, f"{table_name}", 2)

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

# insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

update = PythonOperator(task_id="update", python_callable=update, dag=dag)

dummy_start >> update