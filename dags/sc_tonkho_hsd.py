from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.providers.tableau.operators.tableau_refresh_workbook import TableauRefreshWorkbookOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='TonKho_HSD'
prefix='SC_'

name1='TonKho'
prefix1='SC'




dag_params = {
    'owner': 'airflow',
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
          schedule_interval= '*/10 12,23 * * *',
          tags=[prefix+name, 'Daily', '11']
)

def tonkho_hsd():
    csv_path = '/usr/local/airflow/plugins'+'/'
    datenow = datetime.now().strftime("%Y%m%d")

    # Đọc data

    PXKKVCNB = pd.read_csv(csv_path+f'{prefix1}{name1}/'+f'{datenow}_'+"pr_IN_PXKKVCNB_BI.csv", dayfirst=True)
    Transaction = pd.read_csv(csv_path+f'{prefix1}{name1}/'+f'{datenow}_'+"pr_IN_RawdataTransaction_BI.csv", dayfirst=True)
    RawdataXNTByLot = pd.read_csv(csv_path+f'{prefix1}{name1}/'+f'{datenow}_'+"pr_IN_RawdataXNTByLot_BI.csv", dayfirst=True)
    HDD = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vTlHJ6SB5QNdaHdEaJlRnU7nKcLI2Haj6YlebHtqMvJ-GKsmAZWRvWa5j5dKBY8INF2vSd1fSJlTrXs/pub?gid=0&single=true&output=csv', dayfirst=True, parse_dates=['Ngay(dd/mm/yyyy)'])

    # xử lý RawdataXNTByLot
    RawdataXNTByLot = RawdataXNTByLot[['branchid', 'branchname',
        'invtid','tensanpham', 'lotsernbr',
        'expdate','toncuoi',
            'sltreohoadonao', 'sltreochuataohoadon','siteid', 'tenkho'
    ]].copy()
        # Loai bo ton co chu V
    dk1  = RawdataXNTByLot['invtid'].str[0] != "V"
    RawdataXNTByLot = RawdataXNTByLot[dk1]
    RawdataXNTByLot['tonao'] = RawdataXNTByLot.sltreohoadonao + RawdataXNTByLot.sltreochuataohoadon
    drop_cols(RawdataXNTByLot, ['sltreohoadonao','sltreochuataohoadon'])
    RawdataXNTByLot['datatype'] = 'xnt'
    RawdataXNTByLot.columns = ['mactycn', 'tenctycn', 'masanpham', 'tensanpham','solo','ngayhethan','soluong','makho', 'tenkho', 'tonao', 'datatype']

    # xử lý Transaction
    dk1 = Transaction['trangthai'] == 'Chờ Xử Lý'
    dk2 = Transaction['nghiepvu'] == 'Nhập Mua Hàng'
    Transaction = Transaction[dk1&dk2]
    Transaction['tonao'] = 0
    Transaction['datatype'] = 'gdk'
    Transaction = Transaction[['mactycn', 'tenctycn',
        'masanpham','tensanpham', 'solot',
        'ngayhethan','soluong','makho', 'tenkho', 'tonao', 'datatype'
    ]].copy()
    Transaction['ngayhethan'] = pd.to_datetime(Transaction['ngayhethan'], dayfirst=True)
    # Transaction['solot'] = pd.to_numeric(Transaction['solot'])
    Transaction.columns = ['mactycn', 'tenctycn',  'masanpham','tensanpham','solo','ngayhethan','soluong','makho','tenkho','tonao', 'datatype']


    # xử lý PXKKVCNB
    PXKKVCNB = PXKKVCNB[['macongtycn', 'tencongtycn', 'masanpham', 'tensanpham','solot', 'ngayhethan', 'soluong','makhoden','denkho']].copy()
    PXKKVCNB['tonao'] = 0
    PXKKVCNB['datatype'] = 'gdk'
    PXKKVCNB.columns = ['mactycn', 'tenctycn',  'masanpham','tensanpham','solo','ngayhethan','soluong','makho','tenkho','tonao', 'datatype']

    # xử lý hàng đi đường
    lst = ['ngay','masanpham', 'tensanpham', 'soluonghdd1', 'chinhanh', 'solo', 'exdate', 'soluonghdd']
    # lst = ['ngay','masanpham', 'tensanpham', 'chinhanh','soluonghdd1','soluonghdd']
    HDD.columns = lst
    HDD = HDD[HDD.masanpham.notna()]
    HDD.chinhanh.fillna("NA", inplace=True)
    drop_cols(HDD,['soluonghdd1'])
    query = """select * from biteam.d_chi_nhanh"""
    df_base = get_bq_df(query)
    HDD=pd.merge(HDD,df_base,how='left', left_on='chinhanh',right_on='chinhanh_vt')
    HDD['makho'] = 'HDD'
    HDD['tenkho'] = 'HDD'
    HDD['tonao'] = 0
    HDD['datatype'] = 'hdd'
    HDD= HDD[['macongty','tendms', 'masanpham', 'tensanpham','solo', 'exdate','soluonghdd','makho','tenkho','tonao','datatype']].copy()
    HDD.columns = ['mactycn', 'tenctycn',  'masanpham','tensanpham','solo','ngayhethan','soluong','makho','tenkho', 'tonao', 'datatype']

    # Nối các data lại
    df=union_all([RawdataXNTByLot,Transaction,PXKKVCNB,HDD])
    df = pivot(df, ['mactycn','tenctycn','masanpham','tensanpham','solo','ngayhethan','makho','tenkho','datatype'], {'soluong':np.sum, 'tonao':np.sum})

    df['inserted_at']=datetime.now()
    # bq_values_insert(df,"d_sctonkho_hsd",3)

    # Update 02082022 phuonght2
    #xóa data ngày hiện tại
    bq_ex = ''' DELETE FROM biteam.d_sctonkho_hsd WHERE date(inserted_at) = date(current_date); '''

    # execute_bq_query(bq_ex)
    #input data mới
    df['solo'] = pd.to_numeric(df['solo'])
    df['ngayhethan'] = pd.to_datetime(df['ngayhethan'], dayfirst=True)

    bq_values_insert(df,"d_sctonkho_hsd",3)

    # điều kiện input
def update_tonkho_hds():
    dk  = datetime.now().strftime("%H:%M") in {'11:10','12:10','16:40','23:50'}
    # dk = True
    if dk: tonkho_hsd()
    else: print("Not a good time", datetime.now().strftime("%H:%M"))
# End

    

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

update_tonkho_hds = PythonOperator(task_id="update_tonkho_hds", python_callable=update_tonkho_hds, dag=dag)

dummy_start >> update_tonkho_hds

# dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

# tonkho_hsd = PythonOperator(task_id="tonkho_hsd", python_callable=tonkho_hsd, dag=dag)

# dummy_start >> tonkho_hsd