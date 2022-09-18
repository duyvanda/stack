from utils.df_handle import *

# from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from nhan.google_service_drive import get_service_drive
from googleapiclient.http import MediaFileUpload

local_tz = pendulum.timezone("Asia/Bangkok")

prefix='UPLOAD_'
name='FILES'
csv_path = '/usr/local/airflow/plugins'+'/'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2022, 5, 10, tzinfo=local_tz),
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
          schedule_interval= '*/30 6-23 * * *',
          tags=[prefix+name, 'Sync', '30mins']
)

# import xlsxwriter

# csv_path = '/usr/local/airflow/plugins'+'/'
# csv_path = ''
# prefix = ''
# name = ''

def get_files():
    sql_1 = \
    """
    select
    GETDATE() as ThoiGianRutBaoCao,
    --pd.Crtd_DateTime as NgayTaoYCRH,
    pd.BranchID as ChiNhanh,
    pd.PONbr as YCRH,
    --LineRef as DongTrenPo,
    --LoaiNhapHang = 'HangTonKho',
    pd.InvtID as MaSanPham,
    pd.TranDesc as DienGiai,
    pd.SiteID as MaKho,
    ist.Name as TenKho,
    --ph.VendID as MaNhaCC,
    --ph.Status,
    QtyOrd SLGoc,
    QtyRcvd SLThucNhap,
    QtyOrd-QtyRcvd as SLDatHang
    from PO_Detail pd
    INNER JOIN PO_Header ph on
    pd.PONbr = ph.PONbr and
    pd.BranchID = ph.BranchID
    and ph.status in ('M','O')
    INNER JOIN IN_Site ist ON
    pd.SiteID = ist.SiteId
    """

    df1 = get_ms_df(sql_1)

    # df1.to_csv(csv_path+f'{prefix}{name}/test.csv', index=False)
    # csv_path+f'{prefix}{name}/F_TrackingDebt_INS.csv', index=False
    # df1.head()

    assert checkdup(df1, 2, ['ChiNhanh', 'YCRH','MaSanPham']).sum() == 0, "Duplicate found"

    sql_2 = \
    """
    select
    GETDATE() as ThoiGianRutBaoCao,
    pt.Crtd_DateTime as NgayTaoHoaDon,
    pr.BranchID as ChiNhanh,
    pt.SiteID as MaKho,
    ist.Name as TenKho,
    case when pt.PONbr = '' then 'KHONGCOYCRH' else pt.PONbr end as YCRH,
    pt.BatNbr as SoChungTu,
    piv.InvcNbr as SoHoaDon,
    --pt.RcptNbr,
    pr.Status,
    pt.InvtID as MaSanPham,
    pt.TranDesc DienGiai,
    pt.RcptQty as SoLuongChoNhap
    from PO_Trans pt
    INNER JOIN dbo.PO_Receipt pr ON pr.BatNbr = pt.BatNbr AND pr.BranchID = pt.BranchID and pr.Status = 'H'
    INNER JOIN dbo.PO_InVoice piv ON pr.BatNbr = piv.BatNbr AND pr.BranchID = piv.BranchID
    INNER JOIN IN_Site ist ON pt.SiteID = ist.SiteId
    """

    df2 = get_ms_df(sql_2)

    # df2.head()

    df2.YCRH.fillna("KHONGCOYCRH", inplace=True)

    df2.to_excel(csv_path+f'{prefix}{name}/hoadonnhaptam.xlsx' ,sheet_name="Duy", index=False)

    df2 = pivot(df2, ['ChiNhanh','YCRH','MaSanPham'], {'SoLuongChoNhap':np.sum})

    # df2.head()

    df3 = df1.merge(df2, how='left', on=['ChiNhanh','YCRH','MaSanPham'])

    df3.SoLuongChoNhap.fillna(0, inplace=True)

    df3['SoLuongChoNhapConLai'] = df3.SLDatHang - df3.SoLuongChoNhap

    df3['CanXemXet'] = np.where(df3['SoLuongChoNhapConLai'] > 0, "YES", "NO")

    dk = df3['CanXemXet'] == "YES"

    df4 = df3[dk]

    drop_cols(df4, ["CanXemXet"])

    df4['GhepCodeOutput'] = df4['ChiNhanh'] + df4['MaKho'] + df4['TenKho']

    df4.to_excel(csv_path+f'{prefix}{name}/output.xlsx', sheet_name="Duy", index=False)

# from google.oauth2 import service_account
# from googleapiclient import discovery

# Google Sheet connection
# scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive',
#           'https://www.googleapis.com/auth/drive.file']
# jsonfile = 'D:/data_sale/datateam1599968716114-6f9f144b4262.json'
# credentials = service_account.Credentials.from_service_account_file(jsonfile, scopes=scopes)
# service = discovery.build('drive', 'v3', credentials=credentials)

def upload_file1():
    service = get_service_drive()
    folder_id = '1s6-KRfQpFNTZINO0au9ETcEbqxfi_gp7'
    file_name = csv_path+f'{prefix}{name}/output.xlsx'
    file_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    file_body={'name':'output.xlsx','addParents':[folder_id]}
    query = f"parents = '{folder_id}'"
    files = service.files().list(q=query).execute()
    fileid1  = files['files'][0]['id']
    media = MediaFileUpload('{0}'.format(file_name), mimetype=file_type)
    updated_file = service.files().update(fileId=fileid1, body=file_body, media_body=media)
    updated_file.execute()

def upload_file2():
    service = get_service_drive()
    folder_id = '1s6-KRfQpFNTZINO0au9ETcEbqxfi_gp7'
    file_name = csv_path+f'{prefix}{name}/hoadonnhaptam.xlsx'
    file_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    file_body={'name':'hoadonnhaptam.xlsx','addParents':[folder_id]}
    query = f"parents = '{folder_id}'"
    files = service.files().list(q=query).execute()
    fileid2  = files['files'][1]['id']
    media = MediaFileUpload('{0}'.format(file_name), mimetype=file_type)
    updated_file = service.files().update(fileId=fileid2, body=file_body, media_body=media)
    updated_file.execute()

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

get_files = PythonOperator(task_id="get_files", python_callable=get_files, dag=dag)

upload_file1 = PythonOperator(task_id="upload_file1", python_callable=upload_file1, dag=dag)

upload_file2 = PythonOperator(task_id="upload_file2", python_callable=upload_file2, dag=dag)

dummy_start >> get_files >> upload_file1 >> upload_file2