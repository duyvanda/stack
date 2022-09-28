
from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.tableau.operators.tableau_refresh_workbook import TableauRefreshWorkbookOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='TonKho'
prefix='SC'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2021, 10, 1, tzinfo=local_tz),
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
          schedule_interval= '*/15 11,12,15,23 * * *',
          tags=[prefix+name, 'Daily', '11_PM']
)

def wd():
    if datetime.now().weekday() == 5:
        return 2
    else:
        return 1

def tonkho():
    # x = wd() 
    x = 2 if datetime.now().weekday() == 5 else 1
    csv_path = '/usr/local/airflow/plugins'+'/'
    datenow = datetime.now().strftime("%Y%m%d")
    datenow_add1 = (datetime.now() + timedelta(days=x)).strftime("%Y%m%d")
    fdom = datetime.now().replace(day=1).strftime("%Y%m%d")

    # Xu ly data XNT
    query = f"EXEC pr_IN_RawdataXNTByLot_BI '{datenow}', '{datenow_add1}'"
    df1 = get_ms_df(sql=query)

    df1.columns = cleancols(df1)
    df1.columns = lower_col(df1)

    df1.to_csv(csv_path+f'{prefix}{name}/'+f'{datenow}_'+'pr_IN_RawdataXNTByLot_BI.csv')


    df1 = df1[['siteid', 'invtid',
        'tensanpham', 'stkunit', 'toncuoi',
        'sltreohoadonao', 'sltreochuataohoadon']].copy()

    # df1.dtypes

    # Loai bo ton co chu V
    dk1 = df1['invtid'].str[0] != "V"
    df1 = df1[dk1]

    df1['tonao'] = df1.sltreohoadonao + df1.sltreochuataohoadon

    drop_cols(df1, ['sltreohoadonao','sltreochuataohoadon'])

    df1['datatype'] = 'xnt'

    df1.columns = ['makho', 'masanpham', 'tensanpham', 'donvi', 'toncuoi', 'tonao', 'datatype']

    # Xu ly data GDK
    query2 = f"EXEC pr_IN_RawdataTransaction_BI '{fdom}','{datenow}'"
    df2 = get_ms_df(sql=query2)

    df2.columns = cleancols(df2)
    df2.columns = lower_col(df2)

    
    df2.to_csv(csv_path+f'{prefix}{name}/'+f'{datenow}_'+'pr_IN_RawdataTransaction_BI.csv')

    # df2 = pd.read_csv('20220118_pr_IN_RawdataTransaction_BI.csv')

    # df2.dtypes

    # Dieu kien Cho Xy Ly & Nhap Mua Hang
    dk1 = df2['trangthai'] == 'Chờ Xử Lý'
    dk2 = df2['nghiepvu'] == 'Nhập Mua Hàng'
    df2 = df2[dk1&dk2]

    lst = ['makho','masanpham','tensanpham','donvi','soluong']

    df2 = df2[lst]

    # df2.dtypes

    df2['tonao'] = 0
    df2['datatype'] = 'gdk'

    # df2.to_clipboard()

    # Map new data 27/12, add HDD tu bao cao HDD
    query2a = f"EXEC pr_IN_PXKKVCNB_BI '{fdom}','{datenow}'"
    _df = get_ms_df(sql=query2a)
    _df.columns = cleancols(_df)
    _df.columns = lower_col(_df)
    _df.to_csv(csv_path+f'{prefix}{name}/'+f'{datenow}_'+'pr_IN_PXKKVCNB_BI.csv')

    # _df.columns

    # _df = pd.read_csv('20220118_pr_IN_PXKKVCNB_BI.csv')

    # _df.columns

    lst = ['makhoden','masanpham','tensanpham','donvi','soluong']
    _df = _df[lst]
    lst = ['makho','masanpham','tensanpham','donvi','soluong']
    _df.columns = lst
    _df['tonao'] = 0
    _df['datatype'] = 'gdk'

    df2 = union_all([df2, _df])
    df2 = pivot(df2, ['makho','masanpham','tensanpham','donvi','datatype'], {'soluong':np.sum, 'tonao':np.sum})
    lst = ['makho','masanpham','tensanpham','donvi','soluong','tonao','datatype']
    df2 = df2[lst]
    # df2 = df2.merge(_df, how='left', on=['makho','masanpham'])
    # del(_df)
    # df2['soluonghdd'].fillna(0, inplace=True)
    # df2['soluong'] = df2['soluong'] + df2['soluonghdd']
    # drop_cols(df2,['soluonghdd'])
    df2.columns = ['makho', 'masanpham', 'tensanpham', 'donvi', 'toncuoi', 'tonao','datatype']

    df3 = union_all([df1,df2])

    dfsc = get_bq_df("select makho, hangdiduong,chinhanh,phanloaicn,songaynhan from biteam.d_sc_kho_chi_nhanh")

    df4 = df3.merge(dfsc, how="left", on="makho")

    dk1 = df4['phanloaicn'].notna()

    df4 = df4[dk1].copy()

    dk1 = df4['datatype']=='gdk'

    df4['phanloaicn'] = np.where(dk1, "HDD", df4['phanloaicn'])

    dk0 = df4['phanloaicn'] == 'HDD'
    dk1 = df4['phanloaicn'] == "CN"
    dk2 = df4['phanloaicn'] == "HCM"
    dk3 = df4['phanloaicn'] == "MERAP"
    dk4 = df4['phanloaicn'] == "VIME"

    huy_lst_cn = ['WH0017','WH125','WH0126','WH0127','WH0128','WH0129','WH0130','WH0131','WH0142','WH0145']
    huy_lst_hcm = ['WH0032']
    dk5 = df4['makho'].isin(huy_lst_cn)
    dk6 = df4['makho'].isin(huy_lst_hcm)



    df4['tonhangdiduong'] = np.where(dk0, df4['toncuoi'], 0)
    df4['toncn'] = np.where(dk1, df4['toncuoi'], 0)
    df4['tonhcm'] = np.where(dk2, df4['toncuoi'], 0)
    df4['tonmerap'] = np.where(dk3, df4['toncuoi'], 0)
    df4['tonvime'] = np.where(dk4, df4['toncuoi'], 0)
    df4['toncn_huy'] = np.where(dk5, df4['toncuoi'], 0)
    df4['tonhcm_huy'] = np.where(dk6, df4['toncuoi'], 0)

    df4.to_csv(csv_path+f'{prefix}{name}/'+f'{datenow}_'+'DF4.csv', index=False)


    grouplst = ['masanpham', 'tensanpham', 'donvi', 'chinhanh', 'songaynhan']
    agg_dict = \
    {
    'tonao':np.sum,
    'tonhangdiduong':np.sum,
    'toncn':np.sum,
    'tonhcm':np.sum,
    'tonmerap':np.sum,
    'tonvime':np.sum,
    'toncn_huy':np.sum,
    'tonhcm_huy':np.sum,
    }

    df4.donvi = np.where(df4.donvi == 'Hộp', 'HOP', df4.donvi)
    df4.donvi = np.where(df4.donvi == 'Cái', 'CAI', df4.donvi)

    df5 = pivot(df4, grouplst, agg_dict)

    df5.head()

    df5.to_csv(csv_path+f'{prefix}{name}/'+f'{datenow}_'+'DF5.csv', index=False)

    # df5.dtypes

    assert checkdup(df5,1,['masanpham','chinhanh']).sum() == 0, "MSP & Chi Nhanh khong duoc trung"

    # df5 = pd.read_csv("22012024_df5.csv")
    # df5.chinhanh.fillna("NA", inplace=True)

    # df5.chinhanh.fillna("NA", inplace=True)

    # vc(df5,"chinhanh")

    # Map new data 13/01
    HDDURL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTlHJ6SB5QNdaHdEaJlRnU7nKcLI2Haj6YlebHtqMvJ-GKsmAZWRvWa5j5dKBY8INF2vSd1fSJlTrXs/pub?gid=0&single=true&output=csv'
    _df = pd.read_csv(HDDURL, dayfirst=True, parse_dates=['Ngay(dd/mm/yyyy)'])
    lst = ['ngay','masanpham', 'tensanpham', 'soluonghdd1', 'chinhanh', 'solo', 'exdate', 'soluonghdd']
    # lst = ['ngay','masanpham', 'tensanpham', 'chinhanh','soluonghdd1','soluonghdd']
    _df.columns = lst
    _df = _df[_df.masanpham.notna()]
    _df.chinhanh.fillna("NA", inplace=True)
    drop_cols(_df,['ngay','soluonghdd1', 'solo', 'exdate'])
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
    _df['toncn_huy'] = 0
    _df['tonhcm_huy'] = 0

    _df.columns = ['masanpham','chinhanh','tonhangdiduong','tensanpham','donvi','songaynhan','tonao','toncn','tonhcm','tonmerap','tonvime', 'toncn_huy', 'tonhcm_huy']
    _df = _df[['masanpham', 'tensanpham', 'donvi', 'chinhanh', 'songaynhan', 'tonao', 'tonhangdiduong', 'toncn', 'tonhcm', 'tonmerap', 'tonvime', 'toncn_huy', 'tonhcm_huy']]
    df5 = union_all([df5, _df])
    df5 = pivot(df5, ['masanpham', 'tensanpham', 'donvi', 'chinhanh', 'songaynhan'], {'tonao':np.sum, 'tonhangdiduong':np.sum, 'toncn':np.sum, 'tonhcm':np.sum, 'tonmerap':np.sum, 'tonvime':np.sum, 'toncn_huy':np.sum, 'tonhcm_huy':np.sum})


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
    # dfnmid = get_ps_df("SELECT nhamayid, invtid FROM d_sc_invtid WHERE nhamayid NOTNULL")
    # dfnmid = pd.read_csv('datacodenm.csv')
    # NM = NM.merge(dfnmid, how='left', on='nhamayid')
    # assert NM.invtid.isna().sum() == 0, "NEW NM SKU FOUND"
    # del(dfnmid)
    # NM = NM[NM.invtid.notna()]
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
    NM['toncn_huy'] = 0
    NM['tonhcm_huy'] = 0

    NM = NM[['masanpham', 'tensanpham', 'donvi', 'chinhanh', 'songaynhan', 'tonao', 'tonhangdiduong', 'toncn', 'tonhcm',
        'tonmerap', 'tonvime', 'toncn_huy', 'tonhcm_huy', 'tonnmtp', 'tonnmbt', 'tonnmhh', 'tonnmpo', 'tonnmno']]

    # NM.to_clipboard()

    df5['tonnmtp'] = 0
    df5['tonnmbt'] = 0
    df5['tonnmhh'] = 0
    df5['tonnmpo'] = 0
    df5['tonnmno'] = 0

    # union ton NM va ton DMS
    df5 = union_all([df5, NM])

    # df5.head()

    cur_day = datetime.now().day
    cur_month = datetime.now().month
    cur_year = datetime.now().year

    def get_3m_ago(cur_month=datetime.now().month, cur_year=datetime.now().year):
        if cur_month - 3 < 0:
            return [cur_month + 12 - 3, cur_year -1]
        if cur_month - 3 == 0:
            return [12, cur_year -1]
        else:
            return [cur_month - 3, cur_year]


    # lst = get_3m_ago()
    # start_date = datetime(lst[1], lst[0], cur_day).strftime("%Y%m%d")
    # # start_date
    # query = f"EXEC [pr_OM_RawdataSellOutPayroll_BI_v1] @Fromdate='{start_date}', @Todate='{datenow}'"
    # SALES = get_ms_df(sql=query)
    # SALES.columns = cleancols(SALES)
    # SALES.columns = lower_col(SALES)

    # SALES = SALES[['masanpham', 'soluong', 'makho']]
    # dfsc = get_ps_df("select makho,chinhanh from d_sc_kho_chi_nhanh")
    # dfsc.columns = ['makho','chinhanh_sc']
    # SALES = SALES.merge(dfsc, how="left", on="makho")
    # # SALES.head()
    # del(dfsc)
    # days = len(pd.date_range(start_date, datetime.now()))-13
    # SALES = pivot(SALES, ['masanpham', "chinhanh_sc"], {"soluong":np.sum})
    # SALES['avg_3m'] = round(SALES['soluong']/days,0)
    # # drop_cols(SALES, 'soluong')
    # SALES.columns = ['masanpham','chinhanh','soluong','avg_3m']

    bsql = \
    """
    with sales as 
    (select masanpham,soluong,makho
    from biteam.f_sales
    where date(ngaychungtu)>=date_sub(current_date(),interval 3 month))
    ,
    dfsc as 
    (
    select distinct makho,chinhanh chinhanh_sc
    from biteam.d_sc_kho_chi_nhanh
    )

    select t1.masanpham, t2.chinhanh_sc as chinhanh,sum(t1.soluong) soluong,round(sum(t1.soluong)/(90-13),0) as avg_3m
    from sales t1
    left join dfsc t2 on t1.makho=t2.makho
    group by 1,2
    """

    SALES = get_bq_df(bsql)

    # SALES.to_csv(csv_path+f'{prefix}{name}/'+f'{datenow}_SALES.csv', index=False)


    df6 = df5.merge(SALES, how = "left", on=['masanpham','chinhanh'])

    df6.soluong.fillna(0, inplace=True)
    df6.avg_3m.fillna(0, inplace=True)

    # df6['created_date'] = datetime(2022,1,24)
    df6['created_date'] = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    assert checkdup(df6,1,['masanpham','chinhanh']).sum() == 0, "MSP & Chi Nhanh khong duoc trung"

# Update 25072022 phuonght2
    #xóa data ngày hiện tại
    bq_ex = ''' DELETE FROM biteam.f_sc_daily_invt WHERE date(created_date) = date(current_date); '''
    pg_ex = ''' DELETE FROM f_sc_daily_invt WHERE date(created_date) = date(current_date); '''

    execute_bq_query(bq_ex)
    commit_psql(pg_ex)

    #input data mới
    execute_values_insert(df6,'f_sc_daily_invt')

    df6['inserted_at2'] = datetime.now()
    bq_values_insert(df6, "f_sc_daily_invt", 2)
    execute_bq_query("""call `spatial-vision-343005.view_report.f_tonkhotonghop_daily`();""")

    # điều kiện input
def update_tonkho():
    dk  = datetime.now().strftime("%H:%M") in {'11:00','12:00','14:00','23:45'}
    if True: tonkho()
    else: print("Not a good time", datetime.now().strftime("%H:%M"))
# End

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

update_tonkho = PythonOperator(task_id="update_tonkho", python_callable=update_tonkho, dag=dag)

dummy_start >> update_tonkho
