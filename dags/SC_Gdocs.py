from utils.df_handle import *
from nhan.google_service import get_service
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


local_tz = pendulum.timezone("Asia/Bangkok")

name='Gdocs'
prefix='SC_'
csv_path = '/usr/local/airflow/plugins'+'/'

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
spreadsheets_id = '13Gg1pzdNlsocvt0-Ws41mVoeUUDEsUBOrt5O8J-7aLM'

sql = \
'''

With h_data as ( SELECT
thang,
Upper( Left( TO_CHAR(thang::TIMESTAMP, 'Month') ,3))  AS "monthname",
EXTRACT(month from thang) as month,
EXTRACT(year from thang) as year,
Case WHEN EXTRACT(year from thang::TIMESTAMP) = EXTRACT(year from now()::TIMESTAMP) - 1 then 'ACT LY' ELSE 'ACT TY' end as data_type,
CASE 
WHEN phanam = 'PHA NAM' then 'MDS'
when doanhsochuavat =0 and soluong <> 0 then 'CPA& OTH'
WHEN makenhkh = 'DLPP' THEN 'OTC' ELSE makenhkh 
END as makenhkh,
f_sales.manv,
d_users.tencvbh,
d_users.tenquanlytt,
d_users.tenquanlykhuvuc,
d_users.tenquanlyvung,
macongtycn,
ngaychungtu,
sodondathang,
makhthue,
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
FROM "f_sales"
LEFT JOIN d_users ON
f_sales.manv = d_users.manv
WHERE
LEFT(masanpham,1) != 'V' 
AND f_sales.manv not in ('MA001', 'MA002', 'QUYNHPTA')
AND makenhkh not in ( 'NB')
AND phanam not in ( 'PHA NAM' )
AND EXTRACT(year from thang::TIMESTAMP) >= EXTRACT(year from now()::TIMESTAMP) - 1
),

result_h_data as (

SELECT month,year,masanpham,tensanphamnb,makenhkh,data_type,monthname,sum(soluong) as soluong,sum(doanhsochuavat) as doanhsochuavat from h_data GROUP BY month,year,masanpham,tensanphamnb,makenhkh,data_type,monthname),

sale_days as (
with songay as ( SELECT *,
EXTRACT(month from generate_series) as month,
EXTRACT(year from generate_series) as year,
trim(to_char(generate_series, 'day')) as name,
Case when trim(to_char(generate_series, 'day')) = 'sunday' then 0
		 when trim(to_char(generate_series, 'day')) ='saturday' then 0.5
		 else 1 end as songayban
FROM generate_series(  date_trunc('year', now()) - interval '1 year' ,  
date_trunc('year', now()) + interval '1 year' , '1 day'::interval)  ),

songayban as  ( SELECT month,year,sum(songayban) as songaybantrongthang from songay GROUP BY month,year ),

songaybanmtd as (SELECT month,year,sum(songayban) as songaybanMTD from songay where 
 generate_series::Date <now()::date
GROUP BY month,year )

SELECT a.*,b.songaybanmtd from songayban a
LEFT JOIN songaybanmtd b on a.month=b.month and a.year =b.year
ORDER BY YEAR,month )

SELECT 
A.masanpham||A.makenhkh||A.data_type||A.monthname as "Key",
A.masanpham AS "Mã SP",
A.tensanphamnb AS "Tên SP",
A.makenhkh AS "Kênh",
A.data_type AS "Data type",

A.monthname AS "Tháng",
Case when b.songaybanmtd < b.songaybantrongthang then round(a.soluong / b.songaybanmtd * b.songaybantrongthang ,0) 
else a.soluong end as "Số lượng",
Case 
when a.month = EXTRACT(month from now()) and a.year = EXTRACT(year from now()) and c.soluong  is null then 0
when a.month = EXTRACT(month from now()) and a.year = EXTRACT(year from now()) then  c.soluong 
else a.soluong end as "AVG Số lượng Các Tháng Trước",
a.soluong as "Số lượng MTD"
from result_h_data a
LEFT JOIN sale_days b on a.month = b.month and a.year = b.year
LEFT JOIN ( SELECT masanpham,tensanphamnb,makenhkh,data_type,round(avg(soluong),0) as soluong, round(avg(doanhsochuavat),0) as doanhsochuavat  from result_h_data where year = EXTRACT(year from now()) and month < EXTRACT(month from now()) GROUP BY masanpham,tensanphamnb,makenhkh,data_type ) c on concat(c.masanpham,c.tensanphamnb,c.makenhkh,c.data_type) = concat(a.masanpham,a.tensanphamnb,a.makenhkh,a.data_type)
ORDER BY a.masanpham,a.makenhkh,a.month;
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
    df1 = get_ps_df(sql)
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
