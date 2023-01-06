from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.tableau.operators.tableau_refresh_workbook import TableauRefreshWorkbookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


local_tz = pendulum.timezone("Asia/Bangkok")

name='CustChange'
prefix='Tracking_'
csv_path = '/usr/local/airflow/plugins'+'/'
path = '/usr/local/airflow/dags/files/csv_congno/'
pk_path = '/usr/local/airflow/plugins/Debt_DataDoanhThuPickles/'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2021, 10, 1, tzinfo=local_tz),
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    # 'email':['duyvq@merapgroup.com', 'vanquangduy10@gmail.com'],
    'do_xcom_push': False,
    'execution_timeout':timedelta(seconds=600)
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=10),
}

dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          schedule_interval= '@daily',
          tags=[prefix+name, 'Daily']
)
datenow_1day_ago = ( datetime.now()-timedelta(1) ).strftime("%Y-%m-%d")

def insert():
    # pass
    #start
    sql = \
    f"""
    With #InvoiceCount as
    (
    SELECT * FROM
    (
    SELECT custInvoice.CustID, Invoice.TaxID, ROW_NUMBER()
    OVER (PARTITION BY CustID ORDER BY (SELECT NULL)) AS RowNum
    FROM    AR_Customer_InvoiceCustomer custInvoice WITH(NOLOCK) 
    INNER JOIN AR_CustomerInvoice Invoice WITH(NOLOCK) ON Invoice.CustIDInvoice = custInvoice.CustIDInvoice  
    where Active = 1
    ) t
    WHERE t.RowNum=1
    )

    select
    ch.Version,
    1 as version_id,
    ch.CustID,
    ch.BranchID,
    isnull(ss.Descr, 'UnKnow') as SalesSystem,
    ch.Channel,
    ch.ShopType,
    ch.HCOID,
    ch.HCOType,
    isnull(ch.ClassID, 'UnKnow') as ClassID,
    CheckTerm = CASE ch.CheckTerm WHEN N'N' THEN N'Không Kiểm Tra' WHEN N'D' THEN N'Kiểm Tra Nợ Quá Hạn' ELSE 'UnKnow' END,
    ch.LUpd_Datetime,
    ch.LUpd_User,
    cust.Crtd_Datetime,
    cust.Crtd_User,
    isnull(ch.CustName, 'UnKnow') as CustName,
    isnull(ch.Addr1, 'UnKnow') as Addr1,
    isnull(ch.Attn, 'UnKnow') as Attn,
    isnull(cou.Descr, 'UnKnow') as Country,
    isnull(si.Descr, 'UnKnow') as State,
    isnull(di.Name, 'UnKnow') as District,
    isnull(w.name, 'UnKnow') as Ward,
    isnull(ch.EMailAddr, 'UnKnow') as EMailAddr,
    isnull(ch.Fax, 'UnKnow') as Fax,
    isnull(ch.Phone, 'UnKnow') as Phone,
    isnull(ch.SlsperId, 'UnKnow') as SlsperId,
    --isnull(ch.Status, 'UnKnow') as Status,
    Status = CASE
                WHEN ch.Status = 'A' THEN
                    N'Đang Hoạt Động'
                WHEN ch.Status = 'I' THEN
                    N'Ngưng Hoạt Động'
                WHEN ch.Status = 'H' THEN
                    N'Chờ Xử Lý'
                ELSE 'UnKnow'
            END,
    isnull(ch.Terms, 'UnKnow') as Terms,
    isnull(t.Descr, 'UnKnow') as Territory,
    isnull(ch.Zip, 'UnKnow') as Zip,
    ISNULL(convert(varchar,ch.EstablishDate,23 ),'1900-01-01') as EstablishDate,
    isnull(ch.RefCustID, 'UnKnow') as RefCustID,
    isnull(ch.InActive, 'UnKnow') as InActive,
    isnull(ch.BusinessName, 'UnKnow') as BusinessName,
    isnull(ch.Market, 'UnKnow') as Market,
    isnull(ch.BillMarket, 'UnKnow') as BillMarket,
    isnull(ch.OriCustID, 'UnKnow') as OriCustID,
    isnull(ch.GeneralCustID, 'UnKnow') as GeneralCustID,
    isnull(pay.Descr, 'UnKnow') as PaymentsForm,
    isnull(mag.Descr, 'UnKnow') as GenOrders,
    isnull(ex.Descr, 'UnKnow') as BatchExpForm,
    isnull(ch.CustIdPublic, 'UnKnow') as CustIdPublic,
    isnull(ch.ShoperID, 'UnKnow') as ShoperID,
    isnull(cast (ch.IsAgency as nvarchar), 'UnKnow') as IsAgency,
    isnull(ch.AgencyID, 'UnKnow') as AgencyID,
    isnull(tc.Descr, 'UnKnow') as TaxDeclaration,
    isnull(act.Descr, 'UnKnow') as StockSales,
    isnull(ch.BusinessScope, 'UnKnow') as BusinessScope,
    isnull(ch.LegalName, 'UnKnow') as LegalName,
    ISNULL(convert(varchar,ch.LegalDate,23 ),'1900-01-01') as LegalDate,
    isnull(ch.ChargeReceive, 'UnKnow') as ChargeReceive,
    isnull(ch.ChargePayment, 'UnKnow') as ChargePayment,
    isnull(ch.ChargePhar, 'UnKnow') as ChargePhar,
    cast (CustInvoice1.TaxID as nvarchar) as TaxRegNbr,
    YEAR(cust.Crtd_Datetime) as Year_Created,
    YEAR(ch.LUpd_Datetime) as Year_Updated,
    ch.Addr2, --NO
    ch.BillAddr1,--NO
    ch.BillAddr2,--NO
    ch.BillAttn,--NO
    ch.BillCity,--NO
    ch.BillCountry,--NO
    ch.BillFax,--NO
    ch.BillName,--NO
    ch.BillPhone,--NO
    ch.BillSalut,--NO
    ch.BillState,--NO
    ch.BillZip,--NO
    ch.City,--NO
    ch.CrLmt,--NO
    ch.CrRule,--NO
    ch.CustFillPriority,--NO
    ch.CustType,--NO
    ch.DeliveryID,--NO
    ch.DflSaleRouteID,--NO
    ch.DfltShipToId,--NO
    ch.EmpNum,--NO
    ch.ExpiryDate,--NO
    ch.Exported,--NO
    ch.GracePer,--NO
    ch.LTTContractNbr,--NO
    ch.NodeID,--NO
    ch.NodeLevel,--NO
    ch.ParentRecordID,--NO
    ch.PriceClassID,--NO
    ch.Salut,--NO
    ch.SiteId,--NO
    ch.SupID,--NO
    ch.TaxDflt,--NO
    ch.TaxID00,--NO
    ch.TaxID01,--NO
    ch.TaxID02,--NO
    ch.TaxID03,--NO
    ch.TaxLocId,--NO
    ch.TradeDisc,--NO
    ch.Location,--NO
    ch.Area,--NO
    ch.GiftExchange,--NO
    ch.HasPG,--NO
    ch.Birthdate,--NO
    ch.SellProduct,--NO
    ch.SearchName,--NO
    ch.Classification,--NO
    ch.Chain,--NO
    ch.DeliveryUnit,--NO
    ch.SalesProvince,--NO
    ch.BusinessPic,--NO
    ch.ProfilePic,--NO
    ch.SubTerritory,--NO
    ch.PhotoCode,--NO
    ch.AllowEdit,--NO
    ch.BillWard,--NO
    ch.BillDistrict,--NO
    ch.PPCPassword,--NO
    ch.StandID,--NO
    ch.BrandID,--NO
    ch.DisplayID,--NO
    ch.SizeID,--NO
    ch.TypeCabinets,--NO
    ch.OUnit,--NO
    ch.Mobile,--NO
    ch.LocationCheckType,--NO
    ch.VendorID,--NO
    ch.BuyerID,--NO
    ch.BillTerritory,--NO
    ch.ToDate,--NO
    ch.Limit,--NO
    ch.Account--NO
    from AR_HistoryCustClassID ch
    INNER JOIN dbo.AR_Customer cust WITH (NOLOCK) ON ch.CustID=cust.CustId
    LEFT JOIN dbo.AR_MasterAutoGenOrder mag WITH (NOLOCK)
    ON ch.GenOrders = mag.Code
    LEFT JOIN dbo.AR_MasterBatchExpForm ex WITH (NOLOCK)
    ON ch.BatchExpForm = ex.Code
    LEFT JOIN dbo.SI_District di WITH (NOLOCK)
    ON ch.District = di.District
    AND ch.State = di.State
    LEFT JOIN dbo.SI_Ward w WITH (NOLOCK)
    ON w.Ward = ch.Ward
    AND w.State = ch.State
    AND w.District = ch.District
    LEFT JOIN dbo.SI_State si WITH (NOLOCK)
    ON si.State = ch.State
    LEFT JOIN SI_Country cou WITH(NOLOCK) ON cou.CountryID = ch.Country
    LEFT JOIN SI_Territory t WITH(NOLOCK) ON t.Territory = ch.Territory
    LEFT JOIN dbo.AR_TaxDeclaration tc ON ch.TaxDeclaration=tc.Code
    LEFT JOIN dbo.AR_StockSales act ON act.Code=ch.StockSales
    LEFT JOIN AR_MasterPayments pay WITH(NOLOCK) ON pay.Code = ch.PaymentsForm
    LEFT JOIN dbo.SYS_SalesSystem ss WITH (NOLOCK) ON ch.SalesSystem = ss.Code
    LEFT JOIN #InvoiceCount  CustInvoice1 WITH(NOLOCK) ON CustInvoice1.CustID = ch.CustId 
    where cast(ch.LUpd_Datetime as date) = '{datenow_1day_ago}' order by LUpd_Datetime asc
    """
    print("sql is",sql)
    df = get_ms_df(sql)
    # df.head().to_clipboard()
    lv_df = get_bq_df("""select custid, max(version_id) as lv from biteam.d_master_custhis group by 1""")
    lv_df['lv'] = pd.to_numeric(lv_df['lv'])
    df.columns = lower_col(df)
    df['cum_count'] = df.groupby(['custid']).cumcount()
    df['cum_count'] = df['cum_count'] + 1
    # lv_df[lv_df['custid']=='001733'].head()
    # df['row_num'] = df.index+1
    # df.columns = lower_col(df)
    df2 = df.merge(lv_df, on='custid', how='left')
    df2['lv'].fillna(0, inplace=True)
    # vc(df2, ['lv'])
    # df2[['custid','cum_count','lv']]
    df2['version_id'] = df2['cum_count'] + df2['lv']
    drop_cols(df2, ['cum_count','lv'])
    df2.custfillpriority = df2.custfillpriority.astype(np.float64)
    df2.empnum = df2.empnum.astype(np.float64)
    df2.exported = df2.exported.astype(np.float64)
    df2.graceper = df2.graceper.astype(np.float64)
    df2.nodelevel = df2.nodelevel.astype(np.float64)
    df2.parentrecordid = df2.parentrecordid.astype(np.float64)
    df2.allowedit = df2.allowedit.astype(np.float64)
    df2.isagency = df2.isagency.astype('str')
    df2.slsperid = df2.slsperid.astype('str')
    df2['version_id'] = df2['version_id'].astype(np.int64)
    # df.legaldate = df.legaldate.astype('str')
    # df2['version_id'] = df2['version_id'].astype(np.int64)
    # df2.dtypes
    bq_values_insert(df2, "d_master_custhis", 2)
    print("inserted to d_master_custhis")
    exe_sql = \
    f"""
    DECLARE CHANGED_DATE DATE DEFAULT '{datenow_1day_ago}';
    INSERT INTO biteam.d_tracking_cust_changes
    select * from 
    (
    with cur_version as
    (
    select
    custid,
    version_id,
    version_id -1 as version_lv,
    lupd_datetime,
    lupd_user,
    case 
    when CHR(32) = new_value then "Unknow" 
    when new_value is null then "Unknow"  else new_value end as new_value,
    changetype
    from biteam.d_master_custhis
    UNPIVOT (new_value for changetype in (branchid, salessystem, channel, shoptype, hcoid, hcotype, classid, checkterm, custname, addr1, attn, country, district, emailaddr, fax, phone, slsperid, state, status, terms, territory, zip, establishdate, refcustid, inactive, ward, businessname, market, billmarket, oricustid, generalcustid, paymentsform, genorders, batchexpform, custidpublic, shoperid, isagency, agencyid, taxdeclaration, stocksales, businessscope, legalname, legaldate, chargereceive, chargepayment, chargephar, taxregnbr))
    where date(lupd_datetime) = CHANGED_DATE and version_id > 1
    -- and custid = '001733'

    )
    ,
    las_version as
    (
    select
    custid,
    version_id,
    -- version_id -1 as version_lv,
    -- lupd_datetime,
    case 
    when CHR(32) = old_value then "Unknow" 
    when old_value is null then "Unknow"  else old_value end as old_value,
    changetype
    from biteam.d_master_custhis
    UNPIVOT (old_value for changetype in (branchid, salessystem, channel, shoptype, hcoid, hcotype, classid, checkterm, custname, addr1, attn, country, district, emailaddr, fax, phone, slsperid, state, status, terms, territory, zip, establishdate, refcustid, inactive, ward, businessname, market, billmarket, oricustid, generalcustid, paymentsform, genorders, batchexpform, custidpublic, shoperid, isagency, agencyid, taxdeclaration, stocksales, businessscope, legalname, legaldate, chargereceive, chargepayment, chargephar,taxregnbr))
    -- where custid = '001733'
    )

    select
    cur.*,
    old_value,
    from 
    cur_version cur
    left join las_version las
    ON cur.custid = las.custid
    AND cur.version_lv = las.version_id
    AND cur.changetype = las.changetype
    where new_value != old_value
    )
    """
    execute_bq_query(exe_sql)
    print("Done")

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

dummy_start >> insert
