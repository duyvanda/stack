from utils.df_handle import *

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")
name='MASTER_CUSTOMER'
prefix='DMS_'
path = f'/usr/local/airflow/plugins/{prefix}{name}/'
csv_path = '/usr/local/airflow/plugins'+'/'

# datenow_min1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2022, 4, 14, tzinfo=local_tz),
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    # 'email':['duyvq@merapgroup.com', 'vanquangduy10@gmail.com'],
    'do_xcom_push': False,
    'execution_timeout':timedelta(seconds=300),
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          schedule_interval= '5 7,11,16,23 * * *',
          tags=[prefix+name, 'Sync', 'at0']
)

datenow_min1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
datenow_min0 = (datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d")


sql = \
f"""
With #InvoiceCount as
(
SELECT * FROM
(
SELECT custInvoice.CustIDInvoice, custInvoice.CustID, Invoice.CustNameInvoice, Invoice.CountryID, Invoice.State, 
Invoice.DistrictID, Invoice.Ward, Invoice.ApartNumber, Invoice.StreetName, Invoice.TaxID, Invoice.Phone, Invoice.Email, ROW_NUMBER()
OVER (PARTITION BY CustID ORDER BY (SELECT NULL)) AS RowNum
FROM    AR_Customer_InvoiceCustomer custInvoice WITH(NOLOCK) 
INNER JOIN AR_CustomerInvoice Invoice WITH(NOLOCK) ON Invoice.CustIDInvoice = custInvoice.CustIDInvoice  
where Active = 1
) t
WHERE t.RowNum=1
)
SELECT DISTINCT a.BranchID, 
BranchName= s.CpnyName,
a.CustId, 
a.RefCustID,
--arcic.CustIDInvoice,
--arci.CustNameInvoice,
a.CustName,
Address = CAST ((
                    ISNULL(a.Addr1, '') + CASE ISNULL(a.Addr2, '') WHEN '' THEN '' ELSE ', '+ a.Addr2 END +
                    CASE ISNULL(a.Ward, '') WHEN '' THEN '' ELSE ', ' + ISNULL(w.Name, a.Ward) END +
                    CASE ISNULL(a.District, '') WHEN '' THEN '' ELSE ', '+ ISNULL(dt.Name, a.District) END +
                    CASE ISNULL(a.State, '') WHEN '' THEN '' ELSE ', '+ ISNULL(st.Descr, a.State) END + 
                    CASE ISNULL(a.Country, '') WHEN '' THEN '' ELSE ', '+ ISNULL(cou.Descr, a.Country) END
                    ) AS NVARCHAR(MAX)),
case when a.District = CHAR(32) then NULL else a.District end as DistrictCode,
case when a.State = CHAR(32) then NULL else a.State end as StateCode,
case when a.Ward = CHAR(32) then NULL else a.Ward end as WardCode,
a.Country as CountryCode,
case when a.Territory = 'HCM' then NULL else a.Territory end as TerritoryCode,
a.SalesSystem,
w.Name as WardName,
DistrictDescr = dt.Name,
StateDescr = st.Descr,
TerritoryDescr = t.Descr,
CustInvoice1.[CustIDInvoice],
RTRIM(LTRIM(CustInvoice1.CustNameInvoice)) as CustNameInvoice,
CustInvoice1.Phone as PhoneInvoice,
CustInvoice1.Email as EmailInvoice,
    Addr1 =   
CASE ISNULL(ApartNumber, '') WHEN '' THEN '' ELSE ApartNumber END   +  
--CASE ISNULL(StreetName, '') WHEN '' THEN '' ELSE + IIF(ISNULL(ApartNumber, '') = '', '', ', ') + StreetName END +  
CASE ISNULL(CustInvoice1.Ward, '') WHEN '' THEN '' ELSE ', ' + ISNULL(war.Name, CustInvoice1.Ward) END +  
CASE ISNULL(CustInvoice1.DistrictID, '') WHEN '' THEN '' ELSE ', '+ ISNULL(dis.Name, CustInvoice1.DistrictID) + ', ' END +  
--CASE CustInvoice1.State WHEN '13' THEN ''  
--       WHEN '15' THEN ''  
--       WHEN '24' THEN ''  
--       WHEN '30' THEN ''  
--       WHEN '28' THEN N'Thành Phố' + ' '   
--       else N'Tỉnh' + ' ' end +  --20210505 trung ht bỏ ',' đằng trước tỉnh
CASE ISNULL(CustInvoice1.State, '') WHEN '' THEN '' ELSE ISNULL(sta.Descr, CustInvoice1.State) END +  
CASE ISNULL(CustInvoice1.CountryID, '') WHEN '' THEN '' ELSE ', '+ ISNULL(coun.Descr, CustInvoice1.CountryID) END  ,
TaxRegNbr = CustInvoice1.TaxID ,
a.Channel,
a.ShopType,
ShopTypeDescr = isnull(sh.Descr, ''),
a.Phone,
HCOTypeName = hco.HCOTypeName,
HCOTypeID = a.HCOTypeID,
a.ClassId,
PaymentsForm = pay.Descr,
Terms = isnull(sterm.Descr, ''),
Active = case when a.[Status] = 'A' then N'Active' when a.[Status] = 'I' then N'Inactive' else 'Pending' end, --ISNULL(dbo.fr_Language(ap.LangStatus,@LangID), a.Status),
InActive = a.InActive,
AutoGenOrder = case when a.GenOrders ='KTHD' then N'Không tạo hóa đơn'
                when a.GenOrders ='MDHMHD' then N'Một đơn hàng một hóa đơn'
                when a.GenOrders ='THDHKM' then N'Tách hóa đơn hàng KM'
                WHEN a.GenOrders ='THDTTS' then N'Tách hóa đơn theo thuế suất'
                when a.GenOrders ='THDTSP' then N'Tách hóa đơn từng sản phẩm'
                end,
a.Attn,
a.BusinessScope,
a.BusinessName,
cast(a.LegalDate as date) as LegalDate,
tc.Descr as TaxDeclaration,
act.Descr as StockSales,
--update 04012023
a.IsAgency,
a.AgencyID,
ac.CustName as AgencyName,
ss.Descr as SalesSystemDescr,
a.CheckTerm,
a.ShoperID,
a.Addr1 as Streetname,
ch.Descr as ChannelDescr,
a.HCOID,
a.LegalName,
a.ChargeReceive,
a.ChargePayment,
a.ChargePhar,
--end update 04012023
arcl.Lat,
arcl.Lng,
a.Crtd_Datetime,
a.LUpd_Datetime
FROM AR_Customer a WITH(NOLOCK)
INNER JOIN SYS_Company s WITH(NOLOCK) ON s.CpnyID = a.BranchID
LEFT JOIN #InvoiceCount  CustInvoice1 WITH(NOLOCK) ON CustInvoice1.CustID = a.CustId 
LEFT JOIN SI_Country cou WITH(NOLOCK) ON cou.CountryID = a.Country
LEFT JOIN SI_State st WITH(NOLOCK) ON st.State = a.State AND st.Country = a.Country
LEFT JOIN SI_District dt WITH(NOLOCK) ON a.District = dt.District AND a.Territory = dt.Territory AND dt.State = st.State AND dt.Country = a.Country
LEFT JOIN dbo.SI_Ward w WITH(NOLOCK) ON w.State = a.State AND w.Ward = a.Ward AND w.District = a.District
LEFT JOIN SI_Territory t WITH(NOLOCK) ON t.Territory = a.Territory
LEFT JOIN AR_ShopType sh WITH(NOLOCK) ON sh.Code = a.ShopType
LEFT JOIN SI_Terms sterm WITH(NOLOCK) ON sterm.TermsID = a.Terms
LEFT JOIN AR_MasterPayments pay WITH(NOLOCK) ON pay.Code = a.PaymentsForm
LEFT JOIN AR_HCOType hco WITH(NOLOCK) ON hco.HCOTypeID = a.HCOTypeID AND  hco.HCOID = a.HCOID
LEFT JOIN dbo.AR_HCO ah ON ah.HCOID=a.HCOID
LEFT JOIN SI_ApprovalFlowStatus ap  WITH(NOLOCK) ON ap.Status = a.Status AND ap.AppFolID = 'AR20400'
LEFT JOIN SI_Country as coun WITH(NOLOCK) on coun.CountryID = CustInvoice1.CountryID  
LEFT JOIN SI_State as sta WITH(NOLOCK) on  sta.Country = CustInvoice1.CountryID AND sta.State = CustInvoice1.State  
LEFT JOIN SI_District as dis WITH(NOLOCK) on  dis.District = CustInvoice1.DistrictID  
LEFT JOIN SI_Ward as war WITH(NOLOCK)  on  war.Country = CustInvoice1.CountryID AND war.State = CustInvoice1.State AND war.District = CustInvoice1.DistrictID AND war.Ward = CustInvoice1.Ward
LEFT JOIN AR_CustomerLocation as arcl WITH(NOLOCK)  on  arcl.CustID = a.CustID AND arcl.BranchID = a.BranchID
LEFT JOIN dbo.AR_TaxDeclaration tc ON a.TaxDeclaration=tc.Code
LEFT JOIN dbo.AR_StockSales act ON act.Code=a.StockSales
LEFT JOIN dbo.AR_Customer ac WITH (NOLOCK) ON ac.CustId = a.AgencyID
LEFT JOIN dbo.SYS_SalesSystem ss WITH (NOLOCK) ON a.SalesSystem = ss.Code
LEFT JOIN dbo.AR_Channel ch WITH (NOLOCK) ON ch.Code = a.Channel
"""

# print(sql)

def update_customer():
    df = get_ms_df(sql)
    df['inserted_at'] = datetime.now()
    df.columns = lower_col(df)
    # df.legaldate =  pd.to_datetime(df.legaldate, errors='coece')
    df.statecode = df.statecode.astype('float')
    df.districtcode = df.districtcode.astype('float')
    df.wardcode = df.wardcode.astype('float')
    df.countrycode = df.countrycode.astype('float')
    df.territorycode = df.territorycode.astype('float')
    df.salessystem = df.salessystem.astype('float')
    df.businessscope.replace('', None, inplace=True)
    G_list = ['CHUOI','CLC1','CLC2','CLC3', 'CLC4', 'INS1','INS2','INS3']
    G_Cust = ['P4724-0337']
    dk1 = df.shoptype.isin(G_list)
    dk2 = df.custid.isin(G_Cust)
    df['GType'] = np.where(dk1 | dk2,"G", "No-G")
    # df['custnameinvoice'] = df['custnameinvoice'].str.strip()
    df.to_pickle(path+datenow_min0+'df.pickle')
#     df.businessscope = df.businessscope.str.replace('05','1').str.replace('06','2').str.replace('07','3').str.replace('08','4').str.replace('09','5')

    
def insert():
    df = pd.read_pickle(path+datenow_min0+'df.pickle')
    try:
        print("data shape", df.shape)
        assert df.shape[0] >0
    except AssertionError:
        print("No customer changed")
    else:
        bq_values_insert(df, "d_master_khachhang", 3)
        print("Process Done")

def update_d_gpp():
    sql = \
        """
        select 
        custid as makhachhangdms,
        case when extract(YEAR from date(legaldate)) > 2262 then date('2262-01-01')
        else date(legaldate) end as thoihanhieulucgdpgpp
        from biteam.d_master_khachhang
        """
    df = get_bq_df(sql)
    df.thoihanhieulucgdpgpp = pd.to_datetime(df.thoihanhieulucgdpgpp)
    df['inserted_at'] = datetime.now()
    bq_values_insert(df, "d_gpp", 3)

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

update_customer = PythonOperator(task_id="update_customer", python_callable=update_customer, dag=dag)

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

update_d_gpp = PythonOperator(task_id="update_d_gpp", python_callable=update_d_gpp, dag=dag)

dummy_end = DummyOperator(task_id="dummy_end", dag=dag)

dummy_start >> update_customer >> insert >> update_d_gpp >> dummy_end
