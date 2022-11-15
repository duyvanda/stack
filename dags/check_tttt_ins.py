from utils.df_handle import *
from nhan.google_service import get_service
from google.cloud import bigquery
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import os
import pandas_gbq



local_tz = pendulum.timezone("Asia/Bangkok")

name='Gdocs'
prefix='TTTT_INS'
csv_path = '/usr/local/airflow/plugins'+'/nhan/'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f"{csv_path}spatial-vision-343005-340470c8d77b.json"

dag_params = {
    'owner': 'nhanvo',
    "depends_on_past": False,
    'start_date': datetime(2021, 10, 1, tzinfo=local_tz),
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
        #   https://crontab.guru/
        #   @once
          schedule_interval= '15 5 * * *',
          tags=[prefix+name, 'Daily', 'at 5h15']
)
# Json_file ="D:/ipynb/sever_tttt_ins/datateam1599968716114-6f9f144b4262.json"

service = get_service()

spreadsheets_id = '1csSgQ4xamqJx5zbgJJAtSNhjTaLiAZtrAwppEFlzNUA'
rangeAll = '{0}!A:AA'.format('Check Sale Input')
body = {}

# database = 'biteam'
# host = '171.235.26.161'
# username = 'biteam'
# password = '123biteam'


# conn = psycopg2.connect(
#     host= host,
#     database= database,
#     user= username,
#     password= password)

selectQuery =\
"""
with data_khachhang as (select refcustid,max(lupd_datetime)  as max_lupd_datetime
from `spatial-vision-343005.biteam.d_master_khachhang`group by 1 ),

master_khachhang as (select  distinct a.refcustid,a.custname
from `spatial-vision-343005.biteam.d_master_khachhang` a
JOIN data_khachhang b on b.refcustid = a.refcustid and a.lupd_datetime =b.max_lupd_datetime
 --where upper(trim(active)) ='ACTIVE'
)
SELECT a.manv,
Case when a.manv ='MR0292' then 'Đỗ Thị Hồng Thủy' 
        when a.manv ='MR2629' then 'Nguyễn Thị Mỹ Thanh'
        when a.manv ='MR1432' then 'Lương Tấn Khả'
        when a.manv ='MR2663' then 'Hà Bảo Châu'
else
c.tencvbh end as tennhanvien,a.makhcu,d.custname as tenkhachhang,
a.thongtinthanhtoan,a.giaiphap,a.thoigiangoi,a.input_name
 from `spatial-vision-343005.biteam.d_tttt_ins` a
JOIN (
SELECT makhcu,max(thoigiangoi) as thoigiangoi  FROM `spatial-vision-343005.biteam.d_tttt_ins` GROUP BY 1 ) b
on a.makhcu =b.makhcu and a.thoigiangoi =b.thoigiangoi
LEFT JOIN `spatial-vision-343005.biteam.d_users` c on a.manv = c.manv
LEFT JOIN master_khachhang d on d.refcustid = a.makhcu
where a.thoigiangoi >='2022-05-01'
ORDER BY a.thoigiangoi desc,a.manv
"""
def run_sql():
    bigqueryClient = bigquery.Client()
    df = bigqueryClient.query(selectQuery).to_dataframe()
    df['thoigiangoi'] = df['thoigiangoi'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return(df)

def clear_data():
    service.spreadsheets().values().clear( spreadsheetId=spreadsheets_id, range=rangeAll,body=body ).execute()

def insert_data():
    df1 = run_sql()
    service.spreadsheets().values().append(
    spreadsheetId=spreadsheets_id,
    valueInputOption='RAW',
    range='Check Sale Input!A1',
    body=dict(
        majorDimension='ROWS',
        values=df1.T.reset_index().T.values.tolist())
    ).execute()

def clear_data_ggsheet():
    URL_saleinput ='https://docs.google.com/spreadsheets/d/e/2PACX-1vSYWdHL8hh3RoQTe4dAaidvJ83sbrKiZm9gseWgNru2uWRtfpKzYY87ix9rjl5AK5F7Fa3VGpGA4QgQ/pub?gid=1035878545&single=true&output=csv'
    service = get_service()
    # spreadsheets_id ='1csSgQ4xamqJx5zbgJJAtSNhjTaLiAZtrAwppEFlzNUA'
    now = datetime.now() # current date and time
    check_day = now.day
    df_clear = pd.read_csv(URL_saleinput,header = 0)
    report_spreadsheets=df_clear.Spreadsheets_id
    stt = df_clear.STT
    if check_day == 1:
        for spreadsheets_id in report_spreadsheets:
            rangeAll = '{0}!E2:J1000'.format('Sales Input')
            body = {}
            # print(stt)
            service.spreadsheets().values().clear( spreadsheetId=spreadsheets_id, range=rangeAll,body=body ).execute()
            print('Đã hết số') 
    else:
        print('ngày hiện tại khác ngày đầu tháng nên k clear') 
    if check_day == 1:
        rangeAll = '{0}!C2:J1000'.format('Admin Input')
        spreadsheets_id ='1csSgQ4xamqJx5zbgJJAtSNhjTaLiAZtrAwppEFlzNUA' #admininput
        body = {}
        service.spreadsheets().values().clear( spreadsheetId=spreadsheets_id, range=rangeAll,body=body ).execute()
        print('Đã hết số')     
    else:
        print('ngày hiện tại khác ngày đầu tháng nên k clear')


def data_doanhthu():
    URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSGypBRX7kznYkPB7r3XyODg1YaOhEfMirttGT6JZuDJCBc7O27nU3Cw4HHrklYhDX41eMqK5TBA_3a/pub?gid=0&single=true&output=csv'
    df =pd.read_csv(URL,dtype={'Số hóa đơn':str})
    df.columns = cleancols(df)
    df.columns = lower_col(df)
    df['sotien']=df['sotien'].str.replace(' ','')
    df['sotien']=df['sotien'].astype(int)
    df['ngaythuge'] =  pd.to_datetime(df['ngaythuge'], format='%m/%d/%Y')
    df['inserted_at'] = datetime.now()
    df['updated_at'] = datetime.now()
    df.rename({'ngaythuge':'ngaythu_ge'}
          ,axis='columns',inplace =True)
    df1=df[['ngaythu_ge','chungtu','makhcu','madonhang','sohoadon','sotien','updated_at']]
    return (df1)

def update_table():
	df = data_doanhthu()
	pk = ['makhcu','madonhang','sohoadon','ngaythu_ge','chungtu']
	execute_values_upsert(df, "d_doanhthu_donhang",pk) 


def insert_bq():
    table_name ='d_doanhthu_donhang'
    sql_psql =\
        f"""
        SELECT * FROM {table_name} 
        """
    
    df_psql=get_ps_df(sql_psql)
    pandas_gbq.to_gbq(df_psql, f'biteam.{table_name}', project_id='spatial-vision-343005',if_exists='replace', table_schema = None)



def main():
    clear_data()
    insert_data()
    clear_data_ggsheet()
    update_table() #update bảng data doanh thu INS input 
    insert_bq()

if __name__ == "__main__":
    main()
    
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

py_main= PythonOperator(task_id="main", python_callable=main, dag=dag)

dummy_start >> py_main
# >> tab_refresh

