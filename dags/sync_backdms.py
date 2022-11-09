from ast import Raise
from utils.df_handle import *

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")

name='BACK'
prefix='SYNC_'
csv_path = '/usr/local/airflow/plugins'+'/'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2022, 5, 16, tzinfo=local_tz),
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
          schedule_interval= '0 2 * * *',
          tags=[prefix+name, 'Daily', '60mins']
)

start_date = '2022-01-01'
datenow = datetime.now().strftime("%Y-%m-%d")
datenow_mns1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
fdom = datetime.now().replace(day=1).strftime("%Y%m%d")
datenow_mns45 = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

def update_sync_dms_pda_so():
    try:
        table_name = "sync_dms_pda_so"
        from_tb = "OM_PDASalesOrd"
        status_sql = \
        f"""
        select Status, count(*) as count FROM {from_tb}  where CAST(Crtd_DateTime as DATE) >= '{start_date}' group by Status
        """
        countdms = get_ms_df(status_sql)
        # countdms.head() datenow_mns45
        print("count status ", countdms)

        # count * only
        status_sql_2 = \
        f"""
        select count(*) as count FROM {from_tb}  where CAST(Crtd_DateTime as DATE) >= '{start_date}'
        """
        countdms_2 = get_ms_df(status_sql_2)
        print("count * ", countdms_2)


        statusbq_sql = \
        f"""
        SELECT status as Status, count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}' group by status
        """
        countbq = get_bq_df(statusbq_sql)
        print("count status ", countbq)

        
        dfs = dfs_diff(countbq, countdms)
        assert dfs.shape[0] == 0, "NO DIFF"
        print("NO DIFF")
    except AssertionError:
        sql = \
        f"""
        DECLARE @from DATE = '{datenow_mns45}'
        DECLARE @to DATE = '2022-01-31'
        SELECT
        CONCAT(BranchID,OrderNbr) as pk,
        BranchID,
        OrderNbr,
        CustID,
        SlsPerID,
        Status,
        Crtd_Prog,
        Crtd_User,
        Crtd_DateTime,
        LUpd_DateTime,
        Lupd_User,
        Remark,
        InsertFrom,
        BranchRouteID,
        SalesRouteID,
        DeliveryTime,
        OriOrderNbrUp,
        Version,
        OrderType
        from {from_tb}
        where CAST(Crtd_DateTime as DATE) >= @from
        """
        df = get_ms_df(sql)
        df['inserted_at'] = datetime.now()
        sql = \
        f"""
        delete from biteam.{table_name} where date(crtd_datetime) >= '{datenow_mns45}'
        """
        print("delete_sql: ", sql)
        execute_bq_query(sql)
        bq_values_insert(df, f"{table_name}", 2)
        #recheck count *
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}'
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        ##
# update_sync_dms_pda_so()

def update_sync_dms_so():
    try:
        table_name = "sync_dms_so"
        from_tb = "OM_SalesOrd"
        status_sql = \
        f"""
        select Status, count(*) as count FROM {from_tb}  where CAST(Crtd_DateTime as DATE) >= '{start_date}' group by Status
        """
        countdms = get_ms_df(status_sql)
        # countdms.head()
        print(countdms)

        # count * only
        status_sql_2 = \
        f"""
        select count(*) as count FROM {from_tb}  where CAST(Crtd_DateTime as DATE) >= '{start_date}'
        """
        countdms_2 = get_ms_df(status_sql_2)
        print("count * ", countdms_2)

        statusbq_sql = \
        f"""
        SELECT status as Status, count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}' group by status
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        dfs = dfs_diff(countbq, countdms)
        assert dfs.shape[0] == 0, "NO DIFF"
        print("NO DIFF")
    except AssertionError:
        sql = \
        f"""
        DECLARE @from DATE = '{datenow_mns45}'
        DECLARE @to DATE = '2022-01-31'
        SELECT
        CONCAT(BranchID, OrderNbr) as pk,
        BranchID,
        OrderNbr,
        ARBatNbr,
        ARRefNbr,
        CustID,
        INBatNbr,
        INRefNbr,
        InvcNbr,
        InvcNote,
        OrderDate,
        OrderType,
        OrigOrderNbr,
        SlsPerID,
        Status,
        Terms,
        Crtd_Prog,
        Crtd_User,
        Crtd_DateTime,
        LUpd_DateTime,
        Lupd_User,
        Remark,
        PaymentsForm,
        ContractID,
        InvoiceCustID,
        SalesOrderType,
        ReplForOrdNbr,
        Version,
        AccumulateAmt,
        OrdAmt
        from {from_tb}
        where CAST(Crtd_DateTime as DATE) >= @from
        """
        df = get_ms_df(sql)
        df['inserted_at'] = datetime.now()
        sql = \
        f"""
        delete from biteam.{table_name} where date(crtd_datetime) >= '{datenow_mns45}'
        """
        print("delete_sql: ", sql)
        execute_bq_query(sql)
        bq_values_insert(df, f"{table_name}", 2)
        #recheck count *
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}'
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        ##
def update_sync_dms_ib():
    try:
        table_name = "sync_dms_ib"
        from_tb = "OM_IssueBook"
        status_sql = \
        f"""
        select Status, count(*) as count FROM {from_tb}  where CAST(Crtd_DateTime as DATE) >= '{start_date}' group by Status
        """
        countdms = get_ms_df(status_sql)
        # countdms.head()
        print(countdms)
        statusbq_sql = \
        f"""
        SELECT status as Status, count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}' group by status
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        dfs = dfs_diff(countbq, countdms)
        assert dfs.shape[0] == 0, "NO DIFF"
        print("NO DIFF")
    except AssertionError:
        sql = \
        f"""
        DECLARE @from DATE = '{datenow_mns45}'
        DECLARE @to DATE = '2022-01-31'
        SELECT
        CONCAT(BranchID,BatNbr) as pk,
        BranchID,
        BatNbr,
        DeliveryUnit,
        SlsperID,
        TruckID,
        Status,
        IssueDate,
        Printed,
        Crtd_DateTime,
        Crtd_Prog,
        Crtd_User,
        LUpd_DateTime
        from {from_tb}
        where CAST(Crtd_Datetime as DATE) >= @from
        """
        df = get_ms_df(sql)
        df['inserted_at'] = datetime.now()
        sql = \
        f"""
        delete from biteam.{table_name} where date(crtd_datetime) >= '{datenow_mns45}'
        """
        print("delete_sql: ", sql)
        execute_bq_query(sql)
        bq_values_insert(df, f"{table_name}", 2)
        #recheck count *
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}'
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        ##
def update_sync_dms_dv():
    try:
        table_name = "sync_dms_dv"
        from_tb = "OM_Delivery"
        status_sql = \
        f"""
        select Status, count(*) as count FROM {from_tb}  where CAST(Crtd_DateTime as DATE) >= '{start_date}' group by Status
        """
        countdms = get_ms_df(status_sql)
        # countdms.head()
        print(countdms)
        statusbq_sql = \
        f"""
        SELECT status as Status, count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}' group by status
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        dfs = dfs_diff(countbq, countdms)
        assert dfs.shape[0] == 0, "NO DIFF"
        print("NO DIFF")
    except AssertionError:
        # pass
        sql = \
        f"""
        DECLARE @from DATE = '{datenow_mns45}'
        DECLARE @to DATE = '2022-01-31'
        SELECT
        CONCAT(BranchID,BatNbr,OrderNbr,Sequence) as pk,
        BranchID,
        BatNbr,
        OrderNbr,
        Sequence,
        SlsperID,
        Status,
        Crtd_DateTime,
        Crtd_Prog,
        Crtd_User,
        LUpd_DateTime
        from {from_tb}
        where CAST(Crtd_Datetime as DATE) >= @from
        """
        df = get_ms_df(sql)
        #pk from mds
        df1 = df['pk'].copy()
        df1.to_csv(csv_path+f'{prefix}{name}/'+f'{datenow}_'+'dv_dms.csv', index=False)
        ##
        df['inserted_at'] = datetime.now()
        sql = \
        f"""
        delete from biteam.{table_name} where date(crtd_datetime) >= '{datenow_mns45}'
        """
        print("delete_sql: ", sql)
        execute_bq_query(sql)
        bq_values_insert(df, f"{table_name}", 2)

        #recheck count *
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}'
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        ##
        
        #pk from bq
        bq_sql = \
        f"""
        SELECT pk from biteam.{table_name} where date(crtd_datetime) >= '{datenow_mns45}'
        """
        df1 = get_bq_df(bq_sql)
        df1.to_csv(csv_path+f'{prefix}{name}/'+f'{datenow}_'+'dv_bq.csv', index=False)
        ##


# HANDLE NO STATUS TABLES

def update_sync_dms_pda_sod():
    try:
        table_name = "sync_dms_pda_sod"
        from_tb = "OM_PDASalesOrdDet"
        status_sql = \
        f"""
        select count(*) as count FROM {from_tb}  where CAST(Crtd_DateTime as DATE) >= '{start_date}'
        """
        countdms = get_ms_df(status_sql)
        # countdms.head()
        print(countdms)
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}'
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        dfs = dfs_diff(countbq, countdms)
        assert dfs.shape[0] == 0, "NO DIFF"
        print("NO DIFF")
    except AssertionError:
        # pass datenow_mns45
        sql = \
        f"""
        DECLARE @from DATE = '{datenow_mns45}'
        DECLARE @to DATE = '2022-01-31'
        SELECT
        CONCAT(BranchID,OrderNbr,LineRef) as pk,
        BranchID,
        OrderNbr,
        LineRef,
        InvtID,
        LineQty,
        OrderType,
        OrigOrderNbr,
        SiteID,
        SlsPrice,
        Crtd_Prog,
        Crtd_User,
        Crtd_Datetime,
        LUpd_Datetime,
        SlsperID,
        BeforeVATPrice,
        BeforeVATAmount,
        AfterVATPrice,
        AfterVATAmount,
        VATAmount,
        FreeItem
        from {from_tb}
        where CAST(Crtd_DateTime as DATE) >= @from
        """
        df = get_ms_df(sql)
        df['inserted_at'] = datetime.now()
        sql = \
        f"""
        delete from biteam.{table_name} where date(crtd_datetime) >= '{datenow_mns45}'
        """
        print("delete_sql: ", sql)
        execute_bq_query(sql)
        bq_values_insert(df, f"{table_name}", 2)

        #recheck count *
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}'
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        ##

def update_sync_dms_sod():
    try:
        table_name = "sync_dms_sod1"
        from_tb = "OM_SalesOrdDet"
        status_sql = \
        f"""
        select count(*) as count FROM {from_tb}  where CAST(Crtd_DateTime as DATE) >= '{start_date}'
        """
        countdms = get_ms_df(status_sql)
        # countdms.head()
        print(countdms)
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}'
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        dfs = dfs_diff(countbq, countdms)
        assert dfs.shape[0] == 0, "NO DIFF"
        print("NO DIFF")
    except AssertionError:
        # pass
        sql = \
        f"""
        DECLARE @from DATE = '{datenow_mns45}'
        DECLARE @to DATE = '2022-01-31'
        SELECT
        CONCAT(BranchID, OrderNbr, LineRef) as pk,
        BranchID,
        OrderNbr,
        LineRef,
        FreeItem,
        InvtID,
        LineQty,
        OrderType,
        OrigOrderNbr,
        SiteID,
        Crtd_Prog,
        Crtd_User,
        Crtd_Datetime,
        LUpd_Datetime,
        SlsperID,
        OriginalLineRef,
        BeforeVATPrice,
        BeforeVATAmount,
        AfterVATPrice,
        AfterVATAmount,
        VATAmount,
        DiscAmt,
        DocDiscAmt,
        GroupDiscAmt1
        from {from_tb}
        where CAST(Crtd_DateTime as DATE) >= @from
        """
        df = get_ms_df(sql)
        df['inserted_at'] = datetime.now()
        sql = \
        f"""
        delete from biteam.{table_name} where date(crtd_datetime) >= '{datenow_mns45}'
        """
        print("delete_sql: ", sql)
        execute_bq_query(sql)
        bq_values_insert(df, f"{table_name}", 2)

        #recheck count *
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}'
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        ##

def update_sync_dms_ibd():
    try:
        table_name = "sync_dms_ibd"
        from_tb = "OM_IssueBookDet"
        status_sql = \
        f"""
        select count(*) as count FROM {from_tb}  where CAST(Crtd_DateTime as DATE) >= '{start_date}'
        """
        countdms = get_ms_df(status_sql)
        # countdms.head()
        print(countdms)
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}'
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        dfs = dfs_diff(countbq, countdms)
        assert dfs.shape[0] == 0, "NO DIFF"
        print("NO DIFF")
        raise AssertionError
    except AssertionError:
        # pass
        sql = \
        f"""
        DECLARE @from DATE = '{datenow_mns45}'
        DECLARE @to DATE = '2022-01-31'
        SELECT
        CONCAT(BranchID,BatNbr,OrderNbr) as pk,
        BranchID,
        BatNbr,
        OrderNbr,
        Status,
        DeliveryTime,
        Crtd_DateTime,
        Crtd_Prog,
        Crtd_User,
        LUpd_DateTime,
        Transporters
        from {from_tb}
        where CAST(Crtd_DateTime as DATE) >= @from
        """
        df = get_ms_df(sql)
        df['inserted_at'] = datetime.now()
        sql = \
        f"""
        delete from biteam.{table_name} where date(crtd_datetime) >= '{datenow_mns45}'
        """
        print("delete_sql: ", sql)
        execute_bq_query(sql)
        bq_values_insert(df, f"{table_name}", 2)

        #recheck count *
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name} 
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        ##

def update_sync_dms_iv():
    try:
        table_name = "sync_dms_iv"
        from_tb = "OM_Invoice"
        status_sql = \
        f"""
        select count(*) as count FROM {from_tb}  where CAST(Crtd_DateTime as DATE) >= '{start_date}'
        """
        countdms = get_ms_df(status_sql)
        # countdms.head()
        print(countdms)
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '2022-01-01'
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        dfs = dfs_diff(countbq, countdms)
        assert dfs.shape[0] == 0, "NO DIFF"
        print("NO DIFF")
    except AssertionError:
        # pass
        sql = \
        f"""
        DECLARE @from DATE = '{datenow_mns45}'
        DECLARE @to DATE = '2022-01-31'
        SELECT
        CONCAT(BranchID,RefNbr) as pk,
        BranchID,
        RefNbr,
        InvcNbr,
        InvcNote,
        Crtd_DateTime,
        LUpd_User,
        LUpd_DateTime
        from {from_tb}
        where CAST(Crtd_DateTime as DATE) >= @from
        """
        df = get_ms_df(sql)
        df['inserted_at'] = datetime.now()
        sql = \
        f"""
        delete from biteam.{table_name} where date(crtd_datetime) >= '{datenow_mns45}'
        """
        print("delete_sql: ", sql)
        execute_bq_query(sql)
        bq_values_insert(df, f"{table_name}", 2)

        #recheck count *
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}'
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        ##

def update_sync_dms_err():
    try:
        table_name = "sync_dms_err"
        from_tb = "API_HistoryOM205"
        status_sql = \
        f"""
        select count(*) as count FROM {from_tb}  where Status = 'E' and CAST(DateImport as DATE) >= '{start_date}'
        """
        countdms = get_ms_df(status_sql)
        # countdms.head()
        print(countdms)
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}'
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        dfs = dfs_diff(countbq, countdms)
        assert dfs.shape[0] == 0, "NO DIFF"
        print("NO DIFF")
    except AssertionError:
        # pass
        sql = \
        f"""
        DECLARE @from DATE = '{datenow_mns45}'
        DECLARE @to DATE = '2022-01-31'
        DECLARE @StringApproveOrderT1 INT = ( SELECT LEN(N'Đơn Hàng DH032021-00257 Tồn Tại Các Lỗi:</br>') + 1)
        select
        CONCAT(BranchID,OrderNbr) as pk,
        BranchID,
        OrderNbr,
        DateImport as Crtd_DateTime,
        DateUpdate as LUpd_DateTime,
        REPLACE(
        REPLACE
        (SUBSTRING(ErrorMessage, @StringApproveOrderT1, LEN(ErrorMessage)),
        N'</br>Bạn Không Thể Duyệt Đơn.',
        ''),
        '</br>',
        ', '
        )
        AS ErrorMessage
        from {from_tb}
        where Status = 'E'
        and CAST(DateImport as DATE) >= @from
        """
        df = get_ms_df(sql)
        df['inserted_at'] = datetime.now()
        sql = \
        f"""
        delete from biteam.{table_name} where date(crtd_datetime) >= '{datenow_mns45}'
        """
        print("delete_sql: ", sql)
        execute_bq_query(sql)
        bq_values_insert(df, f"{table_name}", 2)

        #recheck count *
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}'
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        ##

def update_sync_dms_lt():
    try:
        table_name = "sync_dms_lt"
        from_tb = "OM_LotTrans"
        status_sql = \
        f"""
        select count(*) as count FROM {from_tb}  where CAST(Crtd_DateTime as DATE) >= '{start_date}'
        """
        countdms = get_ms_df(status_sql)
        # countdms.head()
        print(countdms)
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}'
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        dfs = dfs_diff(countbq, countdms)
        assert dfs.shape[0] == 0, "NO DIFF"
        print("NO DIFF")
    except AssertionError:
        # pass
        sql = \
        f"""
        DECLARE @from DATE = '{datenow_mns45}'
        DECLARE @to DATE = '2022-01-31'
        SELECT
        CONCAT(BranchID,OrderNbr,LotSerNbr,OMLineRef) as pk,
        BranchID,
        OrderNbr,
        LotSerNbr = ISNULL(LotSerNbr, ''),
        OMLineRef,
        ExpDate,
        Crtd_DateTime,
        Crtd_User,
        LUpd_DateTime,
        LUpd_User
        from {from_tb}
        where CAST(Crtd_DateTime as DATE) >= @from
        """
        df = get_ms_df(sql)
        df['inserted_at'] = datetime.now()
        sql = \
        f"""
        delete from biteam.{table_name} where date(crtd_datetime) >= '{datenow_mns45}'
        """
        print("delete_sql: ", sql)
        execute_bq_query(sql)
        bq_values_insert(df, f"{table_name}", 2)

        #recheck count *
        statusbq_sql = \
        f"""
        SELECT count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}'
        """
        countbq = get_bq_df(statusbq_sql)
        print(countbq)
        ##
        
def update_sync_dms_orddisc():
    from_tb = "OM_OrdDisc"
    table_name = "sync_dms_omorddics"
    # table_temp = "sync_dms_omorddics_temp"
    sql =\
    f"""
    select
    BranchID, 
    --sq.DiscID, 
    --sq.DiscSeq, 
    sq.Descr, 
    sq.DiscIDPN,
    sq.TypeDiscount,
    sq.AccumulateID,
    OrderNbr, 
    --LineRef, 
    --SOLineRef, 
    case when GroupRefLineRef = '' then SOLineRef else GroupRefLineRef end as GroupRefLineRef,
    cast(dis.Crtd_DateTime as date) as Crtd_DateTime,
    cast(dis.LUpd_DateTime as date) as LUpd_DateTime
    from {from_tb} dis
    INNER JOIN dbo.OM_DiscSeq sq WITH (NOLOCK)
    ON sq.DiscID = dis.DiscID
    AND sq.DiscSeq = dis.DiscSeq
    where CAST(dis.Crtd_DateTime as date) >= '{datenow_mns45}'
    """
    df = get_ms_df(sql)
    df.columns = lower_col(df)
    df = pd.concat([df.groupreflineref.str.split(",", expand=True), df], axis=1)
    df_aut = pd.melt(df,id_vars=['branchid','descr', 'discidpn', 'typediscount', 'accumulateid', 'ordernbr', 'groupreflineref', 'crtd_datetime', 'lupd_datetime'], value_name='glineref')
    dk = df_aut['glineref'].notna()
    df=df_aut[dk].copy()
    df['pk'] = df.branchid.astype(str)+"-"+df.ordernbr.astype(str)+"-"+df.glineref.astype(str)
    df['pk2'] = df.branchid.astype(str)+"-"+df.ordernbr.astype(str)+"-"+df.glineref.astype(str)+"-"+df.discidpn.astype(str)
    df = dropdup(df,1, ['pk2'])
    groups=df.groupby(['pk','branchid', 'ordernbr', 'glineref', 'crtd_datetime', 'lupd_datetime'])
    df3=groups['discidpn'].apply(list).reset_index(name='discidpn')
    assert checkdup(df3,2, 'pk').sum() == 0,"Duplicate"
    df3['discidpn1'] = df3['discidpn'].apply(lambda x:x[0])
    df3['discidpn2'] = df3['discidpn'].apply(lambda x:x[1] if(len(x) > 1) else None)
    df3['discidpn3'] = df3['discidpn'].apply(lambda x:x[2] if(len(x) > 2) else None)
    df4=groups['descr'].apply(list).reset_index(name='descr')
    df4['descr1'] = df4['descr'].apply(lambda x:x[0])
    df4['descr2'] = df4['descr'].apply(lambda x:x[1] if(len(x) > 1) else None)
    df4['descr3'] = df4['descr'].apply(lambda x:x[2] if(len(x) > 2) else None)
    df4=df4[['descr','descr1','descr2','descr3']]
    df5=groups['typediscount'].apply(list).reset_index(name='typediscount')
    df5['typediscount1'] = df5['typediscount'].apply(lambda x:x[0])
    df5['typediscount2'] = df5['typediscount'].apply(lambda x:x[1] if(len(x) > 1) else None)
    df5['typediscount3'] = df5['typediscount'].apply(lambda x:x[2] if(len(x) > 2) else None)
    df5=df5[['typediscount','typediscount1','typediscount2','typediscount3']]
    df6 = pd.concat([df3,df4], axis=1)
    df6 = pd.concat([df6,df5], axis=1)
    df6['inserted_at'] = datetime.now()
    del_sql = \
    f"""
    delete from biteam.{table_name} where date(crtd_datetime) >= '{datenow_mns45}'
    """
    print("delete_sql: ", del_sql)
    execute_bq_query(del_sql)
    #df6.shape
    bq_values_insert(df6, table_name, 2)


def update_sync_dms_omapiviettelsecrekey():
    from_tb = "OM_APIViettelSecretKey"
    # from_tb2 = "OM_DeliReportDet"
    table_name = "sync_dms_omapiviettelsecretkey"
    table_temp = "sync_dms_omapiviettelsecretkey_temp"
    # pass
    sql = \
    f"""
    DECLARE @from DATE = '{datenow_mns45}'
    DECLARE @to DATE = '2022-01-31'
    SELECT
    CONCAT(BranchID,BatNbr) as pk,
    BranchID,
    BatNbr,
    InvoiceNbr,
    InvoiceNote,
    TaxInvCode,
    MTLoi,
    Crtd_DateTime,
    Crtd_User,
    LUpd_DateTime,
    LUpd_User
    from {from_tb}
    where
    cast(Crtd_DateTime as DATE) >= @from
    """
    df = get_ms_df(sql)
    df['inserted_at'] = datetime.now()
    sql = \
    f"""
    delete from biteam.{table_name} where date(crtd_datetime) >= '{datenow_mns45}'
    """
    print("delete_sql: ", sql)
    execute_bq_query(sql)
    bq_values_insert(df, f"{table_name}", 2)

    #recheck count *
    statusbq_sql = \
    f"""
    SELECT count(*) as count from biteam.{table_name} where date(crtd_datetime) >= '{start_date}'
    """
    countbq = get_bq_df(statusbq_sql)
    print(countbq)
    ##

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

update_sync_dms_pda_so = PythonOperator(task_id="update_sync_dms_pda_so", python_callable=update_sync_dms_pda_so, dag=dag)

update_sync_dms_so = PythonOperator(task_id="update_sync_dms_so", python_callable=update_sync_dms_so, dag=dag)

update_sync_dms_ib = PythonOperator(task_id="update_sync_dms_ib", python_callable=update_sync_dms_ib, dag=dag)

update_sync_dms_dv = PythonOperator(task_id="update_sync_dms_dv", python_callable=update_sync_dms_dv, dag=dag)

dummy_start1 = DummyOperator(task_id="dummy_start1", dag=dag)

update_sync_dms_pda_sod = PythonOperator(task_id="update_sync_dms_pda_sod", python_callable=update_sync_dms_pda_sod, dag=dag)

update_sync_dms_sod = PythonOperator(task_id="update_sync_dms_sod", python_callable=update_sync_dms_sod, dag=dag)

update_sync_dms_ibd = PythonOperator(task_id="update_sync_dms_ibd", python_callable=update_sync_dms_ibd, dag=dag)

update_sync_dms_iv = PythonOperator(task_id="update_sync_dms_iv", python_callable=update_sync_dms_iv, dag=dag)

update_sync_dms_err = PythonOperator(task_id="update_sync_dms_err", python_callable=update_sync_dms_err, dag=dag)

update_sync_dms_lt = PythonOperator(task_id="update_sync_dms_lt", python_callable=update_sync_dms_lt, dag=dag)

update_sync_dms_orddisc = PythonOperator(task_id="update_sync_dms_orddisc", python_callable=update_sync_dms_orddisc, dag=dag)

update_sync_dms_omapiviettelsecrekey = PythonOperator(task_id="update_sync_dms_omapiviettelsecrekey", python_callable=update_sync_dms_omapiviettelsecrekey, dag=dag)

dummy_start2 = DummyOperator(task_id="dummy_start2", dag=dag)

dummy_end = DummyOperator(task_id="dummy_end", dag=dag)

dummy_start >> [update_sync_dms_pda_so, update_sync_dms_so, update_sync_dms_ib, update_sync_dms_dv] >> dummy_start1
dummy_start1 >> [update_sync_dms_pda_sod, update_sync_dms_sod, update_sync_dms_ibd, update_sync_dms_iv, update_sync_dms_err,update_sync_dms_lt,update_sync_dms_orddisc] >> dummy_start2
dummy_start2 >> update_sync_dms_omapiviettelsecrekey>> dummy_end