from utils.df_handle import *
from nhan.google_service import get_service
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
import pandas_gbq
import os


local_tz = pendulum.timezone("Asia/Bangkok")

name='Gdocs_2023'
prefix='SC_'
csv_path = '/usr/local/airflow/plugins'+'/'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f"{csv_path}/nhan/spatial-vision-343005-340470c8d77b.json"
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

service = get_service()
spreadsheets_id = '1cauiqQ4wS19YV1PTwIlKxpcRA6iv_dtRMwNXGPu7em4'


sql = \
'''

With h_data as ( SELECT
thang,
-- Upper( Left( TO_CHAR(thang, 'Month') ,3))  AS monthname,
Case when EXTRACT(month from thang) = 1 then 'JAN'
     when EXTRACT(month from thang) = 2 then 'FEB'
     when EXTRACT(month from thang) = 3 then 'MAR'
     when EXTRACT(month from thang) = 4 then 'APR'
     when EXTRACT(month from thang) = 5 then 'MAY'
     when EXTRACT(month from thang) = 6 then 'JUN'
     when EXTRACT(month from thang) = 7 then 'JUL'
     when EXTRACT(month from thang) = 8 then 'AUG'
     when EXTRACT(month from thang) = 9 then 'SEP'
     when EXTRACT(month from thang) = 10 then 'OCT'
     when EXTRACT(month from thang) = 11 then 'NOV'
     when EXTRACT(month from thang) = 12 then 'DEC'
     else null end as monthname,
EXTRACT(month from thang) as month,
EXTRACT(year from thang) as year,
Case 
WHEN EXTRACT(year from thang) = 2022 then 'ACT LY'
ELSE 'ACT TY' end as data_type,
CASE 
WHEN phanam = 'PHA NAM' then 'MDS'
when doanhsochuavat =0 and soluong <> 0 then 'CPA& OTH'
WHEN makenhkh = 'DLPP' THEN 'OTC' ELSE makenhkh 
END as makenhkh,
a.manv,
coalesce(d_users.tencvbh,a.tencvbh) as tencvbh,
coalesce(d_users.tenquanlytt,a.tenquanlytt) as tenquanlytt,
coalesce(d_users.tenquanlykhuvuc,a.tenquanlykhuvuc) as tenquanlykhuvuc,
coalesce(d_users.tenquanlyvung,a.tenquanlyvung) as tenquanlyvung,
macongtycn,
ngaychungtu,
sodondathang,
makhcu,
tenkhachhang,
tentinhkh,
makenhphu,
masanpham,
tensanphamviettat,
tensanphamnb,
soluong,
doanhsochuavat,
phanloaispcl,
nhomsp,
khuvucviettat,
chinhanh,
newhco,
phanam
FROM biteam.f_sales a
LEFT JOIN biteam.d_users ON
a.manv = d_users.manv
WHERE
LEFT(masanpham,1) != 'V' 
AND a.manv not in ('MA001', 'MA002', 'QUYNHPTA')
AND makenhkh not in ( 'NB')
AND phanam not in ( 'PHA NAM' )
AND EXTRACT(year from thang) >= 2022
),

result_h_data as (

SELECT month,year,masanpham,tensanphamnb,makenhkh,data_type,monthname,sum(soluong) as soluong,sum(doanhsochuavat) as doanhsochuavat from h_data GROUP BY month,year,masanpham,tensanphamnb,makenhkh,data_type,monthname),

sale_days as (
with songay as ( SELECT *,
EXTRACT(month from generate_series) as month,
EXTRACT(year from generate_series) as year,
extract(dayofweek from generate_series) as name,
-- trim(to_char(generate_series, 'day')) as name,
Case when extract(dayofweek from generate_series) = 1 then 0
		 when extract(dayofweek from generate_series) =7 then 0.5
		 else 1 end as songayban
FROM unnest(generate_date_array( date(date_trunc( (select * from `biteam.d_current_table`),year) - interval 1 year) ,  
date(date_trunc((select * from `biteam.d_current_table`),year) + interval 1 year) , interval 1 day ) ) as generate_series),

songayban as  ( SELECT month,year,sum(songayban) as songaybantrongthang from songay GROUP BY month,year ),

songaybanmtd as (SELECT month,year,sum(songayban) as songaybanMTD from songay where 
 cast(generate_series as date)<(select * from `biteam.d_current_table`)
GROUP BY month,year )

SELECT a.*,b.songaybanmtd from songayban a
LEFT JOIN songaybanmtd b on a.month=b.month and a.year =b.year
ORDER BY YEAR,month  )

SELECT 
A.masanpham||A.makenhkh||A.data_type||A.monthname as Key,
A.masanpham AS ma_sp,
A.tensanphamnb AS ten_sp,
A.makenhkh AS kenh,
A.data_type AS data_type,

A.monthname AS thang,
Case when b.songaybanmtd < b.songaybantrongthang then round(a.soluong / b.songaybanmtd * b.songaybantrongthang ,0) 
else a.soluong end as soluong,
Case 
when a.month = EXTRACT(month from (select * from `biteam.d_current_table`)) and a.year = EXTRACT(year from (select * from `biteam.d_current_table`)) and c.soluong  is null then 0
when a.month = EXTRACT(month from (select * from `biteam.d_current_table`)) and a.year = EXTRACT(year from (select * from `biteam.d_current_table`)) then  c.soluong 
else a.soluong end as avg_soluong_cacthangtruoc,
a.soluong as soluong_mtd
from result_h_data a
LEFT JOIN sale_days b on a.month = b.month and a.year = b.year
LEFT JOIN ( SELECT masanpham,tensanphamnb,makenhkh,data_type,round(avg(soluong),0) as soluong, round(avg(doanhsochuavat),0) as doanhsochuavat  from result_h_data where year = EXTRACT(year from (select * from `biteam.d_current_table`)) and month < EXTRACT(month from (select * from `biteam.d_current_table`)) GROUP BY masanpham,tensanphamnb,makenhkh,data_type ) c on concat(c.masanpham,c.tensanphamnb,c.makenhkh,c.data_type) = concat(a.masanpham,a.tensanphamnb,a.makenhkh,a.data_type)
ORDER BY a.masanpham,a.makenhkh,a.month

'''



dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          # https://crontab.guru/
          # @once 0 0 * * 1,3,5
          schedule_interval= '0 0 * * 1,3,5',
          tags=[prefix+name, 'Nhan', 'Daily', '60mins']
)


def python1():
    rangeAll = '{0}!A:AA'.format('Historical Data')
    body = {}
    service.spreadsheets().values().clear( spreadsheetId=spreadsheets_id, range=rangeAll,body=body ).execute()

# transform

def python2():
    bigqueryClient = bigquery.Client()
    df1 = bigqueryClient.query(sql).to_dataframe()
    service.spreadsheets().values().append(
    spreadsheetId=spreadsheets_id,
    valueInputOption='RAW',
    range='Historical Data!A1',
    body=dict(
        majorDimension='ROWS',
        values=df1.T.reset_index().T.values.tolist())
    ).execute()


dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

python1 = PythonOperator(task_id="python1", python_callable=python1, dag=dag)

python2 = PythonOperator(task_id="python2", python_callable=python2, dag=dag)

dummy_start >> python1 >> python2
