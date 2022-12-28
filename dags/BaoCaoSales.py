
from importlib import resources
from nis import match
from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from tableau import TableauOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


local_tz = pendulum.timezone("Asia/Bangkok")

name='BaoCaoSales'
prefix='Sales'
csv_path = '/home/biserver/data_lake/'+prefix+name+'/'
path = '/usr/local/airflow/dags/files/'

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
          schedule_interval= '*/30 8-17,23-23 * * *',
          tags=[prefix+name, 'Daily', '30mins']
)

def extract_dms():
    print("Success_Extract")

def etl_to_postgres():
    day_ago = 6
    datenow = datetime.now().strftime("%Y-%m-%d")
    datenow_day_ago = ( datetime.now()-timedelta(day_ago) ).strftime("%Y-%m-%d")
    # param_1 = f"'{datenow_day_ago}'"
    # param_2 = f"'20210901'"
    param_3 = f"'{datenow}'"
    # param_4 = f"'20211109'"

    query = f"EXEC [pr_OM_RawdataSellOutPayroll_BI_v1] @Fromdate='{datenow_day_ago}', @Todate={param_3}"

    print("query is ",query)

    FINAL = get_ms_df(sql=query)

    if FINAL.shape[0] != 0 :

        FINAL.columns = cleancols(FINAL)

        if str(FINAL.NgayGiaoHang.dtypes) == 'object' : FINAL.NgayGiaoHang = pd.to_datetime(FINAL.NgayGiaoHang, dayfirst=True)
        
        if str(FINAL.NgayTraHang.dtypes) == 'object' : FINAL.NgayTraHang = pd.to_datetime(FINAL.NgayTraHang, dayfirst=True)

        FINAL.NgayGiaoHang.fillna(datetime(1900, 1, 1), inplace=True)

        FINAL['phanloaispcl'] = FINAL['MaSanPham'].map(
            df_to_dict(pd.read_pickle(path+'spcl.df'))
        ).fillna('Khác')

        FINAL['nhomsp'] = FINAL['MaSanPham'].map(
            df_to_dict(pd.read_pickle(path+'spcl_spm.df'))
        ).fillna('Khác')

        FINAL['khuvucviettat'] = FINAL['TenKhuVuc'].map(
            df_to_dict(pd.read_pickle(path+'d_mkv_viet_tat.df'))
        )

        FINAL['chinhanh'] = FINAL['MaCongTyCN'].map(
            df_to_dict(pd.read_pickle(path+'d_chi_nhanh.df'))
        )

        FINAL['newhco'] = (FINAL['MaKenhPhu']+FINAL['MaPhanLoaiHCO']).map(
            df_to_dict(pd.read_pickle(path+'d_pl_hco.df'))
        )

        FINAL['phanam'] = FINAL['MaSanPham'].map(
            df_to_dict(pd.read_pickle(path+'d_nhom_sp.df'))).fillna('Merap')

        FINAL['thang'] = FINAL['NgayChungTu'] + pd.offsets.Day() - pd.offsets.MonthBegin()

        FINAL['inserted_at'] = datetime.now()

        FINAL['PMT'] = np.where(FINAL['PMT'].isin(['B','C']),'TM','CK')

        # BQ first

        datenow_ = datetime.now().strftime("%Y-%m-%d")

        sql =\
        f"""
        DELETE FROM biteam.f_sales where DATE(ngaychungtu)>='{datenow_day_ago}'
        """

        print("del sql is", sql)

        execute_bq_query(sql)

        bq_values_insert(FINAL, "f_sales", 2)

        # pk = ['macongtycn', 'ngaychungtu', 'sodondathang', 'masanpham', 'solo', 'lineref', 'soluong']

        # execute_values_upsert(FINAL, 'f_sales', pk=pk)
    else:
        print('Not now')


def call_bq_ps():
    execute_bq_query("call view_report.sp_sales()")




dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

extract_dms = PythonOperator(task_id="extract_dms", python_callable=extract_dms, dag=dag)

py_etl_to_postgres = PythonOperator(task_id="etl_to_postgres", python_callable=etl_to_postgres, dag=dag)

call_bq_ps = PythonOperator(task_id="call_bq_ps", python_callable=call_bq_ps, dag=dag)



# hello_task4 = ToCSVMsSqlOperator(task_id='sample-task-4', mssql_conn_id="1_dms_conn_id", sql=sql, database="PhaNam_eSales_PRO", path=path, dag=dag)

# tab_refresh = TableauOperator(task_id='tab_refresh', resource='datasources', method='refresh', find='biteam_daily', match_with='name', dag=dag)


dummy_start >> extract_dms >> py_etl_to_postgres >> [call_bq_ps]
# 
# >> tab_refresh
