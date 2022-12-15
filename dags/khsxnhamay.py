# DON'T USE THIS CELL
from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")
name='nhamay'
prefix='khsx_'
path = f'/usr/local/airflow/plugins/{prefix}{name}/'

# datenow_min1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

dag_params = {
    'owner': 'hanhdt',
    "depends_on_past": False,
    'start_date': datetime(2022, 7, 13, tzinfo=local_tz),
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
          schedule_interval= '@once',
          tags=[prefix+name, '@once', 'hanhdt']
)

# fdom = datetime.now().replace(day=1).strftime("%Y%m%d")
# datenow = datetime.now().strftime("%Y%m%d")
# datenow_add1 = (datetime.now() + timedelta(1)).strftime("%Y%m%d")



def update_khsx_nhamay():
    # t7=pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRD6M-I1HqZYPnrOeRy3q_RKVzIAml-tItrBDUdPUZcX1KGNGHvYL-7dabjV8Ao1IcNRM01pCrGuOb6/pub?gid=0&single=true&output=csv")
    # t8=pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRD6M-I1HqZYPnrOeRy3q_RKVzIAml-tItrBDUdPUZcX1KGNGHvYL-7dabjV8Ao1IcNRM01pCrGuOb6/pub?gid=1038230106&single=true&output=csv")
    # t9=pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRD6M-I1HqZYPnrOeRy3q_RKVzIAml-tItrBDUdPUZcX1KGNGHvYL-7dabjV8Ao1IcNRM01pCrGuOb6/pub?gid=17231151&single=true&output=csv")
    # t10=pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRD6M-I1HqZYPnrOeRy3q_RKVzIAml-tItrBDUdPUZcX1KGNGHvYL-7dabjV8Ao1IcNRM01pCrGuOb6/pub?gid=523968361&single=true&output=csv")
    # t11=pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRD6M-I1HqZYPnrOeRy3q_RKVzIAml-tItrBDUdPUZcX1KGNGHvYL-7dabjV8Ao1IcNRM01pCrGuOb6/pub?gid=507303559&single=true&output=csv")
    t12=pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRD6M-I1HqZYPnrOeRy3q_RKVzIAml-tItrBDUdPUZcX1KGNGHvYL-7dabjV8Ao1IcNRM01pCrGuOb6/pub?gid=604997355&single=true&output=csv")
    t123=pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRD6M-I1HqZYPnrOeRy3q_RKVzIAml-tItrBDUdPUZcX1KGNGHvYL-7dabjV8Ao1IcNRM01pCrGuOb6/pub?gid=1635248494&single=true&output=csv")
    tonchuanhap=pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRyzCqWkMoy_XdBYCO6oirKy6pXwHEjxVKJqBgvmKHXH8dnZ0hl_sOMXxgZkYnl1r2m8tUvz47GPCJZ/pub?gid=0&single=true&output=csv")
    songaynhapkho=pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRyzCqWkMoy_XdBYCO6oirKy6pXwHEjxVKJqBgvmKHXH8dnZ0hl_sOMXxgZkYnl1r2m8tUvz47GPCJZ/pub?gid=770086452&single=true&output=csv")
    quycachdh=pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRyzCqWkMoy_XdBYCO6oirKy6pXwHEjxVKJqBgvmKHXH8dnZ0hl_sOMXxgZkYnl1r2m8tUvz47GPCJZ/pub?gid=1649408062&single=true&output=csv")

    # t7['month']='2022-07-01'
    # t8['month']='2022-08-01'
    # t9['month']='2022-09-01'
    # t10['month']='2022-10-01'
    # t11['month']='2022-11-01'
    t12['month']='2022-12-01'
    t123['month']='2023-01-01'

    month_list = [t12,t123, tonchuanhap,songaynhapkho,quycachdh]
    for i in month_list:
        cleancols(i)
        i.columns = lower_col(i)
    
    df=pd.concat([t12,t123])
    df= df[df['masanphamphanam'].notna()].copy()    
    df_old=df.copy()
    df.drop(['stt','giaidoan',
       'thangtruocchuyensang', 'poton', 'podathang', 'pobosung', 'tong',
       'phanbothuchien','conton'],axis=1,inplace=True)
    df = pd.melt(df,id_vars=['masanphamphanam', 'tensanpham','month'])
    # df= df.apply(lambda x: x.str.replace(',',''))
    # df= df.apply(lambda x: x.str.replace('ĐN',''))
    # df= df.apply(lambda x: x.str.replace('PC',''))
    # df= df.apply(lambda x: x.str.replace('DV',''))
    # df= df.apply(lambda x: x.str.replace('BP',''))
    # df= df.apply(lambda x: x.str.replace('-',''))
    df.value = pd.to_numeric(df.value, errors='coerce')
    df1= df[df['value'].notna()].copy()
    df1= df1[df1['value']!=''].copy()
    #update 28092022
    df1.value = pd.to_numeric(df1.value)
    df1.variable = pd.to_numeric(df1.variable)
    #endupdate
    df1[["value","variable"]]= df1[["value","variable"]].astype('int64')
    df1.rename(columns={'value':'soluong',
                        }, inplace=True)
    df1['ngaysx']= pd.to_datetime(pd.DatetimeIndex(df1['month']).year.astype('str')+'-'+ pd.DatetimeIndex(df1['month']).month.astype('str')+'-'+df1['variable'].astype('str'), format="%Y-%m-%d")
    df1.drop(['variable'],axis=1,inplace=True)

    #add cot conton
    df_old= df_old[['masanphamphanam','month','giaidoan',
       'thangtruocchuyensang', 'poton', 'podathang', 'pobosung', 'tong',
       'phanbothuchien','conton']].copy()
    
    df1['month']=pd.to_datetime(df1['month'], format="%Y-%m-%d")
    df_old['month']=pd.to_datetime(df_old['month'])
    df_old=dropdup(df_old,3,subset=['masanphamphanam','month'])
    df1=pd.merge(df1,df_old,how='left', on=['masanphamphanam','month'])

    tonchuanhap['ngayreview']=pd.to_datetime(tonchuanhap['ngayreview'], format="%d/%m/%Y")
    
    #datenow_1dago = datetime.now().replace(hour=23,minute=30) - timedelta(days=1)


    df1['inserted_at'] = datetime.now()
    tonchuanhap['inserted_at'] = datetime.now()
    songaynhapkho['inserted_at'] = datetime.now()
    quycachdh['inserted_at'] = datetime.now()

    bq_values_insert(df1,"d_nm_kehoachsanxuat",3)
    bq_values_insert(tonchuanhap,"d_nm_tonchuanhap",3)
    bq_values_insert(songaynhapkho,"d_nm_songaynhapkho",3)
    bq_values_insert(quycachdh,"d_sc_quycachdh",3)
    execute_bq_query("""CALL view_report.f_tonkhotonghop_daily();""")

    insert_sql = \
    """
    INSERT INTO biteam.f_kehoachsx_capture_t3
    select * from view_report.f_kehoachsx_capture_t3 where week_day= 2 and day = date_sub(current_date("+7"),interval 1 day)
    """

    execute_bq_query(insert_sql)

# Dont Execute this
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

update_khsx_nhamay = PythonOperator(task_id="update_khsx_nhamay", python_callable=update_khsx_nhamay, dag=dag)
