from utils.df_handle import *

def tonkho():

    print(os.getcwd())

    path = "/usr/local/airflow/dags/files/tonkho/"

    datenow = datetime.now().strftime("%Y%m%d")
    datenow_add1 = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")
    fdom = datetime.now().replace(day=1).strftime("%Y%m%d")

    # datetime.now().replace(day=1)

    # print(datenow)
    # print(datenow_add1)

    query = f"EXEC pr_IN_RawdataXNTByLot_BI '20211221','20211222'"
    df1 = get_ms_df(sql=query)

    # query = f"EXEC pr_IN_RawdataXNTByLot_BI '{datenow}','{datenow_add1}'"
    # df1 = get_ms_df(sql=query)

    df1.columns = cleancols(df1)
    df1.columns = lower_col(df1)
    # ADD NEW
    df1.to_csv(path+f"nxt_{datenow}.csv", index=False)

    # df1.columns

    df1 = df1[['siteid', 'invtid',
        'tensanpham', 'stkunit', 'toncuoi',
        'sltreohoadonao', 'sltreochuataohoadon']].copy()

    # df1.shape

    dk1 = df1['invtid'].str[0] != "V"

    df1 = df1[dk1]

    # df1.shape

    # df1.to_clipboard()

    df1['tonao'] = df1.sltreohoadonao + df1.sltreochuataohoadon

    drop_cols(df1, ['sltreohoadonao','sltreochuataohoadon'])

    df1['datatype'] = 'xnt'

    # df1.to_csv('xntbylot.csv', index=False)

    df1.columns = ['makho', 'masanpham', 'tensanpham', 'donvi', 'toncuoi', 'tonao', 'datatype']

    # df1.to_csv("21xnt.csv")

    query2 = f"EXEC pr_IN_RawdataTransaction_BI '{fdom}','{datenow}'"
    df2 = get_ms_df(sql=query2)

    # vc(df2, 'trangthai')

    # vc(df2, 'nghiepvu')

    df2.columns = cleancols(df2)
    df2.columns = lower_col(df2)
    # ADD NEW
    df2.to_csv(path+f"gdk_{datenow}.csv", index=False)

    dk1 = df2['trangthai'] == 'Chờ Xử Lý'
    dk2 = df2['nghiepvu'] == 'Nhập Mua Hàng'
    df2 = df2[dk1&dk2]

    lst = ['makho','masanpham','tensanpham','donvi','soluong']

    df2 = df2[lst]

    # dk1 = df2['trangthai'] == 'Chờ Xử Lý'
    # dk2 = df2['nghiepvu'] == 'Nhập Mua Hàng'
    # df2 = df2[dk1&dk2]

    # drop_cols(df2, ['trangthai'])

    df2['tonao'] = 0

    df2['datatype'] = 'gdk'

    # Map new data 27/12
    query2a = f"EXEC pr_IN_PXKKVCNB_BI '{fdom}','{datenow}'"
    _df = get_ms_df(sql=query2a)
    _df.columns = cleancols(_df)
    _df.columns = lower_col(_df)
    # ADD NEW
    _df.to_csv(path+f"hdd_{datenow}.csv", index=False)
    lst = ['makhoden','masanpham','soluong']
    _df = _df[lst]
    lst = ['makho','masanpham','soluonghdd']
    _df.columns = lst
    df2 = df2.merge(_df, how='left', on=['makho','masanpham'])
    del(_df)
    df2['soluonghdd'].fillna(0, inplace=True)
    df2['soluong'] = df2['soluong'] + df2['soluonghdd']
    drop_cols(df2,['soluonghdd'])

    df2.columns = ['makho', 'masanpham', 'tensanpham', 'donvi', 'toncuoi', 'tonao','datatype']

    # query3 = f"EXEC pr_IN_PXKKVCNB_BI '20211101','{datenow}'"
    # df3 = get_ms_df(sql=query3)

    df3 = union_all([df1,df2])

    # df3.shape

    dfsc = get_ps_df("select makho, hangdiduong,chinhanh,phanloaicn,songaynhan from d_sc_kho_chi_nhanh")

    df4 = df3.merge(dfsc, how="left", on="makho")

    df1 = df4['phanloaicn'].notna()

    df4 = df4[df1].copy()

    # df4.columns

    dk1 = df4['datatype']=='gdk'

    df4['hangdiduong'] = np.where(dk1, 1, df4['hangdiduong'])

    df4['phanloaicn'] = np.where(dk1, "HDD", df4['phanloaicn'])

    # df4[dk1].head()

    dk1 = df4['hangdiduong'] == 0

    # df4['toncuoi_2'] = np.where(dk1, df4['toncuoi'], 0)

    df4['tonhangdiduong'] = df4['toncuoi'] * df4['hangdiduong']

    # vc(df4, 'phanloaicn')

    dk1 = df4['phanloaicn'] == "CN"
    dk2 = df4['phanloaicn'] == "HCM"
    dk3 = df4['phanloaicn'] == "MERAP"
    dk4 = df4['phanloaicn'] == "VIME"

    df4['toncn'] = np.where(dk1, df4['toncuoi'], 0)
    df4['tonhcm'] = np.where(dk2, df4['toncuoi'], 0)
    df4['tonmerap'] = np.where(dk3, df4['toncuoi'], 0)
    df4['tonvime'] = np.where(dk4, df4['toncuoi'], 0)

    # df4.columns

    grouplst = ['masanpham', 'tensanpham', 'donvi','tonao', 'chinhanh', 'songaynhan']

    agg_dict = \
    {
    'tonhangdiduong':np.sum,
    'toncn':np.sum,
    'tonhcm':np.sum,
    'tonmerap':np.sum,
    'tonvime':np.sum
    }

    df5 = pivot(df4, grouplst, agg_dict)

    # df5.to_csv("df5.csv", index=False)

    # vc(df5, 'tonao')

    # checkdup(df4,1,['masanpham','chinhanh']).sum()

    # df4.to_csv("abc.csv", index=False)

    ### Sales Average

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


    lst = get_3m_ago()
    start_date = datetime(lst[1], lst[0], cur_day).strftime("%Y%m%d")
    # start_date
    query = f"EXEC [pr_OM_RawdataSellOutPayroll_BI_v1] @Fromdate='{start_date}', @Todate='{datenow}'"
    SALES = get_ms_df(sql=query)
    SALES.columns = cleancols(SALES)
    SALES.columns = lower_col(SALES)
    SALES = SALES[['masanpham', 'soluong', 'tentinhkh']]
    DTINH = get_ps_df("select tinh, chinhanh_sc from d_tinh")
    DTINH.columns = ['tentinhkh','chinhanh_sc']
    SALES = SALES.merge(DTINH, how="left", on="tentinhkh")
    del(DTINH)
    days = len(pd.date_range(start_date, datetime.now()))-13

    # SALES.columns

    SALES = pivot(SALES, ['masanpham', "chinhanh_sc"], {"soluong":np.sum})

    # SALES.head()

    SALES['avg_3m'] = round(SALES['soluong']/days,0)

    # SALES.head()

    drop_cols(SALES, 'soluong')

    SALES.columns = ['masanpham','chinhanh','avg_3m']

    df6 = df5.merge(SALES, how = "left", on=['masanpham','chinhanh'])

    df6['tongton'] = df6['toncn'] + df6['tonhcm'] + df6['tonmerap'] + df6['tonvime']

    # df6.columns

    df6['tongton_cong_ao'] = df6['tongton'] + df6['tonao']
    df6['tongton_tatca'] = df6['tongton_cong_ao'] + df6['tonhangdiduong']

    df6['tgban'] = round(df6['tongton_tatca']/df6['avg_3m'],0)

    path = ''

    df6.to_csv(path + f"{datenow}.csv", index=False)



