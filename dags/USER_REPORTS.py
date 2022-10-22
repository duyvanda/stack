# %%
from utils.df_handle import *

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")
name='REPORTS'
prefix='USER_'
path = f'/usr/local/airflow/plugins/{prefix}{name}/'
csv_path = '/usr/local/airflow/plugins'+'/'

# datenow_min1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2022, 4, 14, tzinfo=local_tz),
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    # 'email':['duyvq@merapgroup.com', 'vanquangduy10@gmail.com'],
    'do_xcom_push': False,
    'execution_timeout':timedelta(seconds=300),
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          schedule_interval= '@once',
          tags=[prefix+name, 'update', 'once']
)

# %%
# df = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vSuXl7AnpSA5j_KrgFO2zhLn8DsD20RVuuG7g6Y7F9y5EbiPHT05ug_m3eh8MBOaMDvNmk-DX4g0igF/pub?gid=1043526978&single=true&output=csv")
# cleancols(df)
# df.columns =lower_col(df)
# df_aut = pd.melt(df,id_vars=['tenreport', 'linkreport', 'type','id', 'vw'])

# %%
# df_aut.to_clipboard()

# %%
# df_aut=df_aut[~df_aut['value'].isna()]
# df_aut.rename(columns={'value':'accessgroup'},inplace=True)

# %%
# df_aut.to_clipboard()
# df_group = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vSuXl7AnpSA5j_KrgFO2zhLn8DsD20RVuuG7g6Y7F9y5EbiPHT05ug_m3eh8MBOaMDvNmk-DX4g0igF/pub?gid=2075224509&single=true&output=csv")
# cleancols(df_group)
# df_group.columns=lower_col(df_group)    
# dk2= df_group['csv_link']!='CA'
# df_link = df_group[~df_group['csv_link'].isna()]
# link=df_link['csv_link'][df_link['accessgroupcongtyphongbanlevel']!='CA']

# %%
# link

# %%
# df0 = pd.DataFrame()
# for i in link:
#     df = pd.read_csv(i)
#     df0 = pd.concat([df0,df])
# dropdup(df0,3)
# df0['accessgroup'] = df0['congty']+'-'+df0['phongban']+'-'+df0['chucdanh']
# full_df = pd.merge(df_aut,df0,how='inner', on='accessgroup')

# %%
# full_df.to_clipboard()

# %%
def main():
    #lấy data danh sách report
    df = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vSuXl7AnpSA5j_KrgFO2zhLn8DsD20RVuuG7g6Y7F9y5EbiPHT05ug_m3eh8MBOaMDvNmk-DX4g0igF/pub?gid=1043526978&single=true&output=csv")
    cleancols(df)
    df.columns =lower_col(df)
    df_aut = pd.melt(df,id_vars=['tenreport', 'linkreport', 'type','id', 'vw', 'param'])
    df_aut=df_aut[~df_aut['value'].isna()]
    df_aut.rename(columns={'value':'accessgroup'},inplace=True)

    #lấy data phân quyền detail tách theo phân quyền CA & non CA
    df_group = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vSuXl7AnpSA5j_KrgFO2zhLn8DsD20RVuuG7g6Y7F9y5EbiPHT05ug_m3eh8MBOaMDvNmk-DX4g0igF/pub?gid=2075224509&single=true&output=csv")
    cleancols(df_group)
    df_group.columns=lower_col(df_group)    
    dk2= df_group['csv_link']!='CA'
    df_link = df_group[~df_group['csv_link'].isna()]
    link=df_link['csv_link'][df_link['accessgroupcongtyphongbanlevel']!='CA']
    # ca_link=df_link['csv_link'][df_link['accessgroupcongtyphongbanlevel']=='CA']

    # #Join phân quyền theo Access Group
    df0 = pd.DataFrame()
    for i in link:
        df = pd.read_csv(i)
        df0 = pd.concat([df0,df])
    dropdup(df0,3)
    df0['accessgroup'] = df0['congty']+'-'+df0['phongban']+'-'+df0['chucdanh']
    full_df = pd.merge(df_aut,df0,how='inner', on='accessgroup')

    # #Join phân quyền theo chuc danh đối với cấp CA
    # df1 = pd.DataFrame()
    # for i in ca_link:
    #     df = pd.read_csv(i)
    #     df1 = pd.concat([df1,df])
    # full_df_ca = pd.merge(df_aut,df1,how='inner',left_on='accessgroup', right_on='chucdanh')

    #nối data & inset to BQ
    # full_df = pd.concat([full_df,full_df_ca])
    # df_mail = full_df[['email','tencvbh']]
    # df_mail['email1'] = df_mail['email'].str.lower()
    # df_mail = dropdup(df_mail,3,subset='email1')
    # df_mail['tencvbh1'] =df_mail['tencvbh']
    # df_mail=df_mail[['email','tencvbh1','email1']]
    # full_df = pd.merge(full_df,df_mail,how='left',on='email')
    # full_df.drop(['email','tencvbh'], axis=1,inplace=True)
    # full_df.rename(columns={'email1':'email',
    #                             'tencvbh1':'tencvbh'
    #                             }, inplace=True)


    full_df['inserted_at']=datetime.now()

    # full_df.to_clipboard()
    full_df = dropdup(full_df, 1, subset=['id', 'manv'])
    # bq_values_insert(full_df, "d_phanquyen_tonghop_sep",3)
    # SELECT tenreport, id, manv, type, vw FROM `spatial-vision-343005.biteam.d_phanquyen_tonghop_sep`
    # full_df[['tenreport','id','manv','type','vw', 'param']]
    with open('user_reports.json', 'w', encoding='utf-8') as file:
        full_df[['tenreport','id','manv','type','vw']].to_json(file, force_ascii=False, orient='records')
    upload_file_to_bucket_with_metadata(blobname="public/user_reports.json", file="user_reports.json")

# %%
# client=storage.Client()
# bucketname='django_media_biteam'
# blobname="public/test1.json"
# bucket = client.get_bucket(bucketname)
# blob = bucket.blob(blobname)
# bucket = client.get_bucket('django_media_biteam')
# blob = bucket.blob(blobname)
# blob.cache_control = 'no-cache'
# metadata = {'Cache-Control': 'no-cache'}
# blob.metadata = metadata
# blob.upload_from_filename("user_reports1.json")

# %%
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

main = PythonOperator(task_id="main", python_callable=main, dag=dag)

dummy_end = DummyOperator(task_id="dummy_end", dag=dag)

dummy_start >> main >> dummy_end


