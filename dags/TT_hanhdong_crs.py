import requests
import pandas as pd
from requests.structures import CaseInsensitiveDict
from utils.df_handle import *
from nhan.google_service import get_service
import pandas_gbq
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from datetime import datetime 
import os
import numpy as np
import time
import json
from email.message import Message, EmailMessage
import email
import os
import smtplib
import imaplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

local_tz = pendulum.timezone("Asia/Bangkok")

name='Send_email'
prefix='TT_Hanhdong_CRS_'
csv_path = '/usr/local/airflow/plugins/nhan'+'/'


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
          # https://crontab.guru/
          # @once 0 0 * * 1,3,5
          schedule_interval= '00 8 * * 1',
          tags=[prefix+name, 'On Monday']
)

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f"{csv_path}spatial-vision-343005-340470c8d77b.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/usr/local/airflow/dags/files/bigquery2609.json"

date_now = datetime.now().date()


sql = f""" 


-- Create view view_report.f_data_kh_checkin_canchamsoc
-- tthd_send_email
-- test lan 2 18:46:00
-- as

with data_kh_chua_viengtham as (
  SELECT slsperid,cast(date_trunc(visitdate,month) as date) as thang_,
custid,count(distinct soluong_checkin_thucte) 
from `view_report.f_data_checkin_pbh_v2` 

where visitdate >='2022-08-01'
group by 1,3,thang_ having count(distinct soluong_checkin_thucte) =0),

data_gpp as (
 with data_quydinh_viengtham as (
SELECT distinct slsperid,custid,cast(date_trunc(visitdate,month) as date) as mapping_date FROM `spatial-vision-343005.biteam.sync_dms_salesroutedet`
where 
visitdate >='2022-08-01'
and delroutedet is false
 )

select distinct a.*except (custid),a.custid as makhachhangdms,b.slsperid,d.tencvbh,
b.mapping_date as thang_,
Case when  date(a.legaldate) < (select * from `biteam.d_current_table`) 
    then 1 else 0 end as is_hetthoihanhieuluc,
Case when 
     date_add((select * from `biteam.d_current_table`),interval 30 day) >= date(a.legaldate) 
     and date(a.legaldate) > (select * from `biteam.d_current_table`) 
     then 1 else 0 end as is_saphetthoihanhieuluc,
Case when  date(a.legaldate) < (select * from `biteam.d_current_table`) 
    then a.custid else null end as custid_hetthoihanhieuluc,
Case when 
     date_add((select * from `biteam.d_current_table`),interval 30 day) >= date(a.legaldate) 
     and date(a.legaldate) > (select * from `biteam.d_current_table`) 
     then a.custid else null end as custid_saphetthoihanhieuluc,
    --  c.custname,
     d.tenquanlytt,
     d.tenquanlykhuvuc,
     d.tenquanlyvung,
    --  c.statedescr,
    --  c.territorydescr
 from `biteam.d_master_khachhang` a
LEFT JOIN data_quydinh_viengtham b on a.custid = b.custid 
-- LEFT JOIN `biteam.d_master_khachhang` c on c.custid = a.makhachhangdms
LEFT JOIN `biteam.d_users` d on d.manv =b.slsperid

),

data_kh_quydinh_viengtham as (
SELECT slsperid,cast(date_trunc(visitdate,month) as date) as thang_,
count(distinct custid)as so_kh_quydinh_viengtham FROM `spatial-vision-343005.biteam.sync_dms_salesroutedet`
where 
visitdate >='2022-08-01'
and delroutedet is false
group by 1,2
 ),

 data_tile_dh as 
(
    select slsperid,
cast(date_trunc(visitdate,month) as date) as thang_,
count(distinct sl_dh_thucte) as sl_dh_thucte,
count(distinct sl_kh_checkin) as sl_kh_checkin,
count(distinct so_kh_viengtham_tt)  as so_kh_viengtham_tt,
count(distinct sl_kh_phatsinhdh) as sl_kh_phatsinhdh,
count(distinct soluong_checkin_thucte) as sl_checkin_thucte,
round(safe_divide(count(distinct sl_dh_thucte),count(distinct soluong_checkin_thucte) )*100,0)  as tile_dh
from `view_report.f_data_checkin_pbh_v2`
where
  visitdate >='2022-08-01'
group by 1,2
),

group_data_kh_chua_viengtham as 
 (
   select slsperid,thang_,count(distinct custid) as so_kh_chua_viengtham 
   from data_kh_chua_viengtham
   group by 1,2
 ),
 group_data_gpp as 
 (
   select slsperid,thang_,count(distinct custid_saphetthoihanhieuluc) as custid_saphetthoihanhieuluc,
   count(distinct custid_hetthoihanhieuluc) as custid_hetthoihanhieuluc
   from data_gpp 
   group by 1,2
 ),

data_email as (
 with data_email as (SELECT distinct msnvcsmmoi, emailmoi FROM `spatial-vision-343005.biteam.d_hr_thongtincanhan` ),

filter_email as (
select msnvcsmmoi, emailmoi,row_number() over (partition by msnvcsmmoi order by emailmoi) as row_
from data_email )

select msnvcsmmoi, emailmoi from filter_email where row_ =1 ),

phanquyen as (

    with max_phanquyen as (
  select manv,max(inserted_at) as max_inserted_at from `spatial-vision-343005.biteam.d_phanquyen_trading`
group by 1 ),
result as (
select a.*
from `spatial-vision-343005.biteam.d_phanquyen_trading` a
JOIN max_phanquyen b on a.manv=b.manv and a.inserted_at =b.max_inserted_at 
 where a.trangthaihoatdong='Còn hoạt động')
 select * from result
),


result as (
 select  
  c.slsperid,
  d.tencvbh,
  c.thang_,
  Case when date_trunc((select transaction_date from `biteam.d_current_table` ),month) = date_trunc(c.thang_,month) then 
  Cast ((select transaction_date from `biteam.d_current_table` ) as date)
  
  else (select cast( date((extract(year from c.thang_)),(extract(month from c.thang_)),1) + interval 1 month - interval 1 day as date) from `biteam.d_current_table`) end as thang_den,

  Case  when (select extract(day from transaction_date) from `biteam.d_current_table` ) = 1 
      --when extract(day from date(2022,10,1)) = 1 
      then cast(date_trunc((select transaction_date from `biteam.d_current_table`) - interval 1 day,month) as date)
      --then cast(date_trunc(date(2022,10,1) - interval 1 day,month) as date)
  else Cast (date_trunc((select transaction_date from `biteam.d_current_table` ),month) as date) end as check_thang,

  c.so_kh_quydinh_viengtham,
  Case when a.so_kh_chua_viengtham is null then 0 else a.so_kh_chua_viengtham end as so_kh_chua_viengtham,
  Case when b.custid_saphetthoihanhieuluc is null then 0 else b.custid_saphetthoihanhieuluc end as custid_saphetthoihanhieuluc,
  Case when b.custid_hetthoihanhieuluc is null then 0 else b.custid_hetthoihanhieuluc end as custid_hetthoihanhieuluc,
  Case when e.sl_dh_thucte is null then 0 else e.sl_dh_thucte end as sl_dh_thucte,
  Case when e.sl_checkin_thucte is null then 0 else e.sl_checkin_thucte end as sl_checkin_thucte,
  Case when e.tile_dh is null then 0 else e.tile_dh end as tile_dh,
  Case when e.so_kh_viengtham_tt is null then 0 else e.so_kh_viengtham_tt end as so_kh_viengtham_tt,
  Case when e.sl_kh_checkin is null then 0 else e.sl_kh_checkin end as sl_kh_checkin,
  Case when e.sl_kh_phatsinhdh is null then 0 else e.sl_kh_phatsinhdh end as sl_kh_phatsinhdh,
  -- Case when c.slsperid ='MR1282' then 'nhanvt92@gmail.com' --'trangnn@phanam.com.vn'
  --       when c.slsperid ='MR1995' then 'nhanvt@merapgroup.com'
  --       when c.slsperid ='MR1605' then 'duyvq@merapgroup.com'
  -- else null end as send_email,
  Case 
  when f.emailmoi = 'thaoptt@merapgroup.com' then 'thaopt@merapgroup.com'
  when f.emailmoi is null then 'nhanvt@merapgroup.com' else f.emailmoi end as send_email
 from data_kh_quydinh_viengtham c

 LEFT JOIN group_data_kh_chua_viengtham  a on c.slsperid = a.slsperid and a.thang_=c.thang_
 LEFT JOIN group_data_gpp b on c.slsperid =b.slsperid and c.thang_=b.thang_
 LEFT JOIN data_tile_dh e on e.slsperid = c.slsperid and e.thang_ =c.thang_
 LEFT JOIN `biteam.d_users` d on d.manv =c.slsperid
 LEFT JOIN data_email f on f.msnvcsmmoi = c.slsperid

  JOIN phanquyen g on g.manv = c.slsperid
  where  d.position in ('S') -- and g.manv is null
 )

 select * from result

  where --slsperid in ('MR1282','MR1995','MR1605') and
 thang_ =check_thang 
 --and send_email ='nhanvt@merapgroup.com'

  --and c.thang_ ='2022-09-01'
  --order by thang_ 


"""
def get_data():
    bigqueryClient = bigquery.Client()
    df = bigqueryClient.query(sql).to_dataframe()
    # a=df.shape
    # b=a[0]
    path=f'{csv_path}tt_hanhdong_crs/{date_now}_data_send_email.csv'
    df.to_csv(path,index = False,header=False)
    return None

def sendmail(slsperid, tencvbh,thang_,thang_den,check_thang, so_kh_quydinh_viengtham,so_kh_chua_viengtham, custid_saphetthoihanhieuluc,custid_hetthoihanhieuluc, sl_dh_thucte, sl_checkin_thucte,tile_dh, so_kh_viengtham_tt, sl_kh_checkin, sl_kh_phatsinhdh,send_email):
    EMAIL_ADDRESS = 'bi@merapgroup.com'
    pass_bi = Variable.get("login__pass_bi_mail", None)
    EMAIL_PASSOWRD = pass_bi
    msg = MIMEMultipart('alternative')
    msg['Subject'] = f'Thông tin gợi ý hành động cho CRS'
    msg.add_header('Content-Type','text/html')
    # FROM EMAIL
    msg['From'] = EMAIL_ADDRESS
    # TO EMAIL
    msg['To'] = send_email
    # CC EMAIL
    cc='nhanvt@merapgroup.com'
    # msg['Cc'] = 'nhanvt@merapgroup.com'
    # BODY
    html_str = f"""\
    <!DOCTYPE html>
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Your HTML Title</title>
    <body>
    <h1 style="font-size:100%"> Dear Mr/Ms {tencvbh}:</h1>
    <p>Ngày rà soát: từ {thang_} đến {thang_den}, trong tổng số  <b>{so_kh_quydinh_viengtham}</b>  khách hàng thuộc phạm vi quản lý của bạn thì có:</p>
        <p>    -   <b>{so_kh_chua_viengtham}</b> khách hàng bạn chưa viếng thăm.</p>
        <p>    -   <b>{custid_hetthoihanhieuluc}</b> khách hàng đã hết hạn GPP.</p>
        <p>    -   <b>{custid_saphetthoihanhieuluc}</b> khách hàng có GPP hết hạn trong vòng 30 ngày tới.</p>
    <p>Trong số <b>{sl_kh_checkin}</b> khách hàng bạn đã viếng thăm:</p>
        <p>    -   <b>{so_kh_viengtham_tt}</b> khách hàng là bạn viếng thăm trực tiếp.</p>
        <p>    -   <b>{sl_kh_phatsinhdh}</b> khách hàng phát sinh đơn hàng.</p>
        <p>    -   Tỉ lệ đơn hàng thực tế/số lần viếng thăm thực tế của bạn đến hiện tại là <b>{tile_dh}%</b>.</p>
    <p>Để biết chi tiết vui lòng xem báo cáo ở link sau: <a href="https://ds.merapgroup.com/">https://ds.merapgroup.com/</a></p>
    <p>Đăng nhập tài khoản theo thông tin BI đã gửi qua email, vui lòng chọn: <h1 style="color:rgb(54, 38, 234); font-size:100%"> Báo cáo S | Sales Performance - Tab Viếng thăm (Tổng quan)</h1></p>
    <h1 style="color:rgb(234, 38, 38); font-size:100%"> Đây là tin nhắn tự động. Vui lòng không trả lời Email này!!!</h1>
    </body>
    </html>
    """
    # print(html_str)
    # <!-- <p>Xin Chào: {tencvbh}, Mã số NV {slsperid}</p> -->
    part1 = MIMEText(html_str, 'html')
    msg.attach(part1)
    # msg.add_alternative(html, subtype='html')
    # msg.set_payload(html)
    
    with smtplib.SMTP_SSL('mail.merapgroup.com', 465) as smtp:
        conn_smtp = smtp.login(EMAIL_ADDRESS, EMAIL_PASSOWRD)
        print(conn_smtp)
        smtp.sendmail(to_addrs=(send_email,cc), from_addr=EMAIL_ADDRESS, msg = msg.as_string())
        print('Message has been sent')
        print(send_email)




def send_email_auto():
    # df1=get_data()
    # a=df1.shape
    # b=a[0]
    with open(f"{csv_path}tt_hanhdong_crs/{date_now}_data_send_email.csv",encoding='utf-8') as f:
        # line=f.readlines()[1:]
        len_csv=pd.read_csv(f"{csv_path}tt_hanhdong_crs/{date_now}_data_send_email.csv",encoding='utf-8')
        b=len(len_csv)
        # f.close()
        for i in range(0,b+1):
            try:
                print(i)
                slsperid, tencvbh,thang_,thang_den,check_thang, so_kh_quydinh_viengtham,so_kh_chua_viengtham, custid_saphetthoihanhieuluc,custid_hetthoihanhieuluc, sl_dh_thucte, sl_checkin_thucte,tile_dh, so_kh_viengtham_tt, sl_kh_checkin, sl_kh_phatsinhdh,send_email = f.readline().split(',')
                sendmail(slsperid, tencvbh,thang_,thang_den,check_thang, so_kh_quydinh_viengtham,so_kh_chua_viengtham, custid_saphetthoihanhieuluc,custid_hetthoihanhieuluc, sl_dh_thucte, sl_checkin_thucte,tile_dh, so_kh_viengtham_tt, sl_kh_checkin, sl_kh_phatsinhdh,send_email)
                time.sleep(1)
            except BaseException as E:
                print(E)
            
def main(): 
    get_data()
    send_email_auto()
    
if __name__ == "__main__":
    main()



dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

py_main = PythonOperator(task_id="main", python_callable=main, dag=dag)

dummy_start >> py_main
