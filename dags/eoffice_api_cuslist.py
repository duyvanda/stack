from utils.df_handle import *
from math import ceil
import requests
from vars import vars
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")


name='API_CUSLIST'
prefix='EOFFICE_'
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
          schedule_interval= '10 0 * * *',
          tags=[prefix+name, 'Daily', 'at0']
)


def insert_first_time():
    #*--------------*
    #get pages
    base_url = 'https://eoffice.merapgroup.com/eoffice/api/api/'

    headers = {"Authorization": f"Bearer {vars['name_eoffice']}", "Content-Type":"application/json"}

    extra_url=f'raw/data-follow?&limit=1'
    
    print("url is: ",base_url+extra_url)
    
    r = requests.get(base_url+extra_url, headers=headers)
    
    r.json()
    
    data = r.json()['data']
    
    print("total output: " ,r.json()['total'])
    
    print("do dai list api: ",len(data))
    
    maxpage = ceil(r.json()['total']/500)
    
    pages = [x for x in range(1, maxpage+1, 1)]
    
    limit=500
    #end get pages
    #*--------------*
    for page in pages:
        base_url = 'https://eoffice.merapgroup.com/eoffice/api/api/'
        headers = {"Authorization": f"Bearer {vars['name_eoffice']}", "Content-Type":"application/json"}
        extra_url=f'raw/data-follow?limit={limit}&page={page}'
        r = requests.get(base_url+extra_url, headers=headers)
        r.json()
        data = r.json()['data']
        # len(data)
        df1=pd.DataFrame.from_records(data)
        df1.columns = ['makhdms', 'customer_phone', 'customer_name', 'follow_name',
            'follow_phone', 'gender', 'birthday', 'follow_address',
            'customer_address', 'updated_at', 'customer_role_name', 'office_code',
            'office_name', 'user_name', 'user_code', 'status']
        lst = ['makhdms', 'customer_phone', 'customer_name', 'follow_name',
            'follow_phone', 'follow_address',
            'customer_address','status','updated_at']
        df1=df1[lst]
        vc(df1, 'status')
        dk = df1['status'] != 0
        df1 = df1[dk].copy()
        # drop_cols(df1, 'status')
        print("page inserted: ", page)

        tuple1 = tuple(df1['makhdms'].tolist())+('','')

        if len(tuple1) > 0:

            sql = \
            f"""
            SELECT DISTINCT a.CustId as makhdms, b.Descr AS tenkhtinh FROM AR_Customer a
            INNER JOIN SI_State b ON
            a.State = b.State
            where a.CustId in {tuple1}
            """

            DMS_KH_TINH = get_ms_df(sql)
            df1 = df1.merge(DMS_KH_TINH, how = 'left', on='makhdms')
            
            execute_values_upsert(df1, 'f_eoffice_cus_infor', pk=['follow_phone'])
        else:
            pass


dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

# py_get_csv_files = PythonOperator(task_id="get_csv_files", python_callable=get_csv_files, dag=dag)

insert_first_time = PythonOperator(task_id="insert_first_time", python_callable=insert_first_time, dag=dag)


# hello_task4 = ToCSVMsSqlOperator(task_id='sample-task-4', mssql_conn_id="1_dms_conn_id", sql=sql, database="PhaNam_eSales_PRO", path=path, dag=dag)

# tab_refresh = TableauRefreshWorkbookOperator(task_id='tab_refresh', workbook_name='Báo Cáo Doanh Thu Tiền Mặt', dag=dag)


dummy_start >> insert_first_time
# >> tab_refresh
