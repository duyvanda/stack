### BoilerPlate
from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='TonKhoAdHoc'
prefix='SC_'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2022, 4, 2, tzinfo=local_tz),
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
          schedule_interval= '@once',
          tags=[prefix+name, 'adhoc']
)

csv_path = '/usr/local/airflow/plugins'+'/'
### END BOILERPLATE

# from utils.df_handle import *
# csv_path = ''

x = 2 if datetime.now().weekday() == 0 else 1
datenow = datetime.now().strftime("%Y%m%d")
datenow_add1 = (datetime.now() + timedelta(days=x)).strftime("%Y%m%d")
datenow_mns1 = (datetime.now() - timedelta(days=x)).strftime("%Y%m%d")
fdom = datetime.now().replace(day=1).strftime("%Y%m%d")

# datenow_mns1
def tonkho_adhoc():
    df5 = pd.read_csv(csv_path+f"{datenow_mns1}_DF5.csv")
    df5.chinhanh.fillna("NA", inplace=True)

    # Map new data 13/01
    HDDURL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTlHJ6SB5QNdaHdEaJlRnU7nKcLI2Haj6YlebHtqMvJ-GKsmAZWRvWa5j5dKBY8INF2vSd1fSJlTrXs/pub?gid=0&single=true&output=csv'
    _df = pd.read_csv(HDDURL, dayfirst=True, parse_dates=['Ngay(dd/mm/yyyy)'])

    lst = ['ngay','masanpham', 'tensanpham', 'chinhanh','soluonghdd1','soluonghdd']
    _df.columns = lst
    _df = _df[_df.masanpham.notna()]
    drop_cols(_df,['ngay','soluonghdd1'])
    _df = pivot(_df, ['masanpham', 'chinhanh'], {'soluonghdd':np.sum})

    # Update 11/02/2022
    df5_dict = df5[['masanpham','tensanpham','donvi']].copy()
    df5_dict.drop_duplicates(inplace=True)
    _df = _df.merge(df5_dict, how='left', on='masanpham')
    # Update 11/02/2022
    df5_dict = df5[['chinhanh','songaynhan']].copy()
    df5_dict.drop_duplicates(inplace=True)
    _df = _df.merge(df5_dict, how='left', on='chinhanh')

    _df['tonao'] = 0
    _df['toncn'] = 0
    _df['tonhcm'] = 0
    _df['tonmerap'] = 0
    _df['tonvime'] = 0

    _df.columns = ['masanpham','chinhanh','tonhangdiduong','tensanpham','donvi','songaynhan','tonao','toncn','tonhcm','tonmerap','tonvime']
    _df = _df[['masanpham', 'tensanpham', 'donvi', 'chinhanh', 'songaynhan', 'tonao', 'tonhangdiduong', 'toncn', 'tonhcm', 'tonmerap', 'tonvime']]
    df5 = union_all([df5, _df])
    df5 = pivot(df5, ['masanpham', 'tensanpham', 'donvi', 'chinhanh', 'songaynhan'], {'tonao':np.sum, 'tonhangdiduong':np.sum, 'toncn':np.sum, 'tonhcm':np.sum, 'tonmerap':np.sum, 'tonvime':np.sum})

    assert checkdup(df5,1,['masanpham','chinhanh']).sum() == 0, "MSP & Chi Nhanh khong duoc trung"

    NMURLTP = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRvmeMXGwa-2u-cCQmDRyXs__a8oLfcZk9yLyq1LupmdsvzulMVlxHublEJAKszBY-zmnl_Wm1KNnvZ/pub?gid=0&single=true&output=csv'
    nmtp = pd.read_csv(NMURLTP)
    # nmtp.columns = nmtp.iloc[0]
    headers = nmtp.iloc[0]
    nmtp  = pd.DataFrame(nmtp.values[1:], columns=headers)
    nmtp.columns = cleancols(nmtp)
    nmtp.columns = lower_col(nmtp)
    dk1 = nmtp.ten_phanam.notna()
    nmtp = nmtp[dk1].copy()
    nmtp = nmtp[['ten_phanam','convert']].copy()
    nmtp.convert = pd.to_numeric(nmtp.convert)
    dk2 = nmtp.convert != 0
    nmtp = nmtp[dk2].copy()
    nmtp['datatype'] = 'TP'

    # nmtp.convert.sum()

    NMURLBT = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRvmeMXGwa-2u-cCQmDRyXs__a8oLfcZk9yLyq1LupmdsvzulMVlxHublEJAKszBY-zmnl_Wm1KNnvZ/pub?gid=869914713&single=true&output=csv'
    nmbt = pd.read_csv(NMURLBT)
    nmbt.columns = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]
    nmbt = nmbt[[14,15]].copy()
    nmbt.columns = ['ten_phanam', 'convert']
    dk1 = nmbt.ten_phanam.notna()
    nmbt = nmbt[dk1].copy()
    nmbt.convert = pd.to_numeric(nmbt.convert)
    dk2 = nmbt.convert != 0
    nmbt = nmbt[dk2].copy()
    nmbt['datatype'] = 'BT'

    NMURLHH = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRvmeMXGwa-2u-cCQmDRyXs__a8oLfcZk9yLyq1LupmdsvzulMVlxHublEJAKszBY-zmnl_Wm1KNnvZ/pub?gid=1577909815&single=true&output=csv'
    nmhh = pd.read_csv(NMURLHH)
    nmhh.columns = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]
    nmhh = nmhh[[14,15]].copy()
    nmhh.columns = ['ten_phanam', 'convert']
    dk1 = nmhh.ten_phanam.notna()
    nmhh = nmhh[dk1].copy()
    nmhh.convert = pd.to_numeric(nmhh.convert)
    dk2 = nmhh.convert != 0
    nmhh = nmhh[dk2].copy()
    nmhh['datatype'] = 'HH'

    NM = union_all([nmtp,nmbt,nmhh])
    NM.columns = ['invtid','soluong','datatype']
    NM = pivot(NM, ['invtid', 'datatype'], {'soluong':np.sum})

    df5_dict = df5[['masanpham','tensanpham','donvi']].copy()
    df5_dict.drop_duplicates(inplace=True)

    NM.columns = ['masanpham', 'datatype', 'soluong']

    # ADDING Ton Theo PO & NM no PO
    THEOPO_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRvmeMXGwa-2u-cCQmDRyXs__a8oLfcZk9yLyq1LupmdsvzulMVlxHublEJAKszBY-zmnl_Wm1KNnvZ/pub?gid=574959890&single=true&output=csv'
    nmtheopo = pd.read_csv(THEOPO_URL)

    nmtheopo.columns = cleancols(nmtheopo)
    nmtheopo.columns = lower_col(nmtheopo)

    nmtheopo.columns = ['mapn','dummy','nmnopo','tontheopo']

    nmpo = nmtheopo[['mapn','tontheopo']].copy()
    nmpo['datatype'] = 'PO'
    nmpo = nmpo[['mapn', 'datatype', 'tontheopo']]
    nmpo.columns = ['masanpham', 'datatype', 'soluong']

    nmno = nmtheopo[['mapn','nmnopo']].copy()
    nmno['datatype'] = 'NO'
    nmno = nmno[['mapn', 'datatype', 'nmnopo']]
    nmno.columns = ['masanpham', 'datatype', 'soluong']

    NM = union_all([NM,nmpo,nmno])

    # NM.to_clipboard()

    NM = NM.merge(df5_dict, how='left', on='masanpham')

    NM['chinhanh'] = 'NM'

    dk1 = NM['datatype'] == "TP"
    dk2 = NM['datatype'] == "BT"
    dk3 = NM['datatype'] == "HH"
    dk4 = NM['datatype'] == "PO"
    dk5 = NM['datatype'] == "NO"

    NM['tonnmtp'] = np.where(dk1, NM['soluong'], 0)
    NM['tonnmbt'] = np.where(dk2, NM['soluong'], 0)
    NM['tonnmhh'] = np.where(dk3, NM['soluong'], 0)
    NM['tonnmpo'] = np.where(dk4, NM['soluong'], 0)
    NM['tonnmno'] = np.where(dk5, NM['soluong'], 0)

    grouplst = ['masanpham', 'tensanpham', 'donvi', 'chinhanh']
    agg_dict = \
    {
    'tonnmtp':np.sum,
    'tonnmbt':np.sum,
    'tonnmhh':np.sum,
    'tonnmpo':np.sum,
    'tonnmno':np.sum
    }

    NM = pivot(NM, grouplst, agg_dict)

    NM['songaynhan'] = 0
    NM['tonao'] = 0
    NM['tonhangdiduong'] = 0
    NM['toncn'] = 0
    NM['tonhcm'] = 0
    NM['tonmerap'] = 0
    NM['tonvime'] = 0

    NM = NM[['masanpham', 'tensanpham', 'donvi', 'chinhanh', 'songaynhan', 'tonao', 'tonhangdiduong', 'toncn', 'tonhcm',
        'tonmerap', 'tonvime', 'tonnmtp', 'tonnmbt', 'tonnmhh', 'tonnmpo', 'tonnmno']]

    # NM.to_clipboard()

    df5['tonnmtp'] = 0
    df5['tonnmbt'] = 0
    df5['tonnmhh'] = 0
    df5['tonnmpo'] = 0
    df5['tonnmno'] = 0

    # union ton NM va ton DMS
    df5 = union_all([df5, NM])

    # df5.head()

    SALES=pd.read_csv(csv_path+f'{datenow}_SALES.csv')

    df6 = df5.merge(SALES, how = "left", on=['masanpham','chinhanh'])

    df6.soluong.fillna(0, inplace=True)
    df6.avg_3m.fillna(0, inplace=True)

    df6['created_date'] = ( datetime.now() - timedelta(days=x) ).replace(hour=0, minute=0, second=0, microsecond=0)

    assert checkdup(df6,1,['masanpham','chinhanh']).sum() == 0, "MSP & Chi Nhanh khong duoc trung"

    sel_sql = \
    f"""
    Select * from f_sc_daily_invt WHERE created_date='{datenow_mns1}'
    """

    currentInvt = get_ps_df(sel_sql)

    currentInvt.to_csv(csv_path+f"currentInvt_{datenow_mns1}.csv", index=False)

    del_sql = \
    f"""
    DELETE from f_sc_daily_invt WHERE created_date='{datenow_mns1}'
    """

    commit_psql(del_sql)

    execute_values_insert(df6,'f_sc_daily_invt')

# DAG FLOW
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

tonkho_adhoc = PythonOperator(task_id="tonkho", python_callable=tonkho_adhoc, dag=dag)

dummy_start >> tonkho_adhoc
# END DAG FLOW