# -*- coding: utf-8 -*-
"""f_sales_pend.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1Gv4TJMgxnLP4CFit3NFDFF8kjnBH9Ci6
"""

# DON'T USE THIS CELL
from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")
name='Phanam'
prefix='PhanQuyen_'
path = f'/usr/local/airflow/plugins/{prefix}{name}/'

# datenow_min1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

dag_params = {
    'owner': 'phuonght2',
    "depends_on_past": False,
    'start_date': datetime(2022, 4, 28, tzinfo=local_tz),
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
          schedule_interval= '0 8-17 * * *',
          tags=[prefix+name, 'Daily', '60mins']
)

# fdom = datetime.now().replace(day=1).strftime("%Y%m%d")
# datenow = datetime.now().strftime("%Y%m%d")
# datenow_add1 = (datetime.now() + timedelta(1)).strftime("%Y%m%d")



def update_phanquyen_phanam():
    print("I am at path ->", os.getcwd())
    # df_lv = pd.read_csv(path+'phanquyen_phanam_last_ver.csv')

    bq_lv = '''select t1.*except(inserted_at)
    from biteam.d_phanquyen_phanam t1
    right join (select manv, max(inserted_at) inserted_at from biteam.d_phanquyen_phanam group by 1) t2 
    on t1.manv=t2.manv and t1.inserted_at = t2.inserted_at'''

    df_lv = get_bq_df(bq_lv)
    df_csv1 = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vTt5LlJ0cxfoEldpF4oiyL4c4u0mnuHHVx1_350Ucj21tgTVrS2dN1_ft8wyaBOXPqJ84UVFSJ7idhZ/pub?gid=0&single=true&output=csv")
    df_csv2 = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vTt5LlJ0cxfoEldpF4oiyL4c4u0mnuHHVx1_350Ucj21tgTVrS2dN1_ft8wyaBOXPqJ84UVFSJ7idhZ/pub?gid=1320773847&single=true&output=csv")
    df_csv = pd.concat([df_csv1,df_csv2])

    # drop_cols(df_lv,['Unnamed: 0'])
    # df_csv1=df_csv
    # drop_cols(df_csv1,['Unnamed: 0'])

    df_check_delete = df_csv.rename(columns={"tencvbh": "tencvbh_check"})
    df_lv=pd.merge(df_lv,df_check_delete[['manv','tencvbh_check']],how='left', on='manv')
    df_lv_delete = df_lv[df_lv['tencvbh_check'].isna()]

    df_lv = df_lv.drop(df_lv.index[df_lv_delete.index[df_lv_delete['trangthaihoatdong']=='???? ngh???']])
    df_update = df_lv_delete[df_lv_delete['trangthaihoatdong']!='???? ngh???']
    df_update['trangthaihoatdong']='???? ngh???'
    df_lv.update(df_update)
    drop_cols(df_lv,['tencvbh_check'])
    df = dfs_diff(df_lv,df_csv)
    df = dropdup(df,3,subset='manv')
    df=df[~df['manv'].isna()].copy()
    # df_csv.to_csv(path+'phanquyen_phanam_last_ver.csv')
    if df.shape[0]>0:   
        df['inserted_at'] = datetime.now()
        bq_values_insert(df, "d_phanquyen_phanam",2)
        print(df) 
    else:
        print('no update')


# Dont Execute this
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

update_phanquyen_phanam = PythonOperator(task_id="update_phanquyen_phanam", python_callable=update_phanquyen_phanam, dag=dag)

dummy_start >> update_phanquyen_phanam