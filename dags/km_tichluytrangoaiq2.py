from utils.df_handle import *

# from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='TICHLUYTRANGOAIQ2'
prefix='KM_'
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
          tags=[prefix+name, '30mins']
)

def insert():
    pass

def update():
    sql1 = \
    f"""
    SELECT
    GETDATE() as Inserted_At,
    s.SlsperID,
    r.BranchID,
    r.SlsperID as RouteID,
    r.CustID,
    r.AccumulateID,
    r.LevelID,
    r.Status,
    r.Crtd_DateTime
    FROM dbo.OM_AccumulatedRegis r
    INNER JOIN dbo.OM_SalespersonRoute s ON s.SalesRouteID=r.SlsperID
    WHERE AccumulateID='CSBH22Q2-14QD/MR'
    """
    sql2 = \
    f"""
    SELECT
    CustID,
    AccumulatedValue, 
    Reward FROM dbo.OM_AccumulatedResult
    WHERE AccumulateID='CSBH22Q2-14QD/MR'
    """
    sql3 = \
    f"""
    SELECT
    sa.CustID,
    sum(case when LEFT(sa.OrderNbr,2) = 'CO' then -1*Amt else 1*Amt end) as PaidAmt
    FROM dbo.OM_SalesOrdAccumulate sa 
    INNER JOIN OM_SalesOrd so ON
    sa.OrderNbr = so.OrderNbr and
    sa.BranchID = so.BranchID and
    so.Status = 'C'
    WHERE AccumulateID='CSBH22Q2-14QD/MR' group by sa.CustID
    """
    df1 = get_ms_df(sql1)
    df2 = get_ms_df(sql2)
    df3 = get_ms_df(sql3)
    dfa = df1.merge(df2, on=['CustID'], how='left')
    # df.head()
    dfa = dfa.merge(df3, on=['CustID'], how='left')
    dfa.PaidAmt.fillna(0, inplace=True)

    sql1 = \
    f"""
    SELECT
    GETDATE() as Inserted_At,
    s.SlsperID,
    r.BranchID,
    r.SlsperID as RouteID,
    r.CustID,
    r.AccumulateID,
    r.LevelID,
    r.Status,
    r.Crtd_DateTime
    FROM dbo.OM_AccumulatedRegis r
    INNER JOIN dbo.OM_SalespersonRoute s ON s.SalesRouteID=r.SlsperID
    WHERE AccumulateID='CSBH22Q2-04QD/PN'
    """
    sql2 = \
    f"""
    SELECT
    CustID,
    AccumulatedValue, 
    Reward FROM dbo.OM_AccumulatedResult
    WHERE AccumulateID='CSBH22Q2-04QD/PN'
    """
    sql3 = \
    f"""
    SELECT
    sa.CustID,
    sum(case when LEFT(sa.OrderNbr,2) = 'CO' then -1*Amt else 1*Amt end) as PaidAmt
    FROM dbo.OM_SalesOrdAccumulate sa 
    INNER JOIN OM_SalesOrd so ON
    sa.OrderNbr = so.OrderNbr and
    sa.BranchID = so.BranchID and
    so.Status = 'C'
    WHERE AccumulateID='CSBH22Q2-04QD/PN' group by sa.CustID
    """
    df1 = get_ms_df(sql1)
    df2 = get_ms_df(sql2)
    df3 = get_ms_df(sql3)
    dfb = df1.merge(df2, on=['CustID'], how='left')
    # df.head()
    dfb = dfb.merge(df3, on=['CustID'], how='left')
    dfb.PaidAmt.fillna(0, inplace=True)
    # dfb.shape
    df = union_all([dfa,dfb])
    bq_values_insert(df, "f_tichluytrangoaiq2", 3)

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

update = PythonOperator(task_id="update", python_callable=update, dag=dag)

# update_sync_dms_ardoc_2 = PythonOperator(task_id="update_sync_dms_ardoc_2", python_callable=update_sync_dms_ardoc_2, dag=dag)

dummy_start >> update >> insert