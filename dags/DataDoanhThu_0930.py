from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.tableau.operators.tableau_refresh_workbook import TableauRefreshWorkbookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


local_tz = pendulum.timezone("Asia/Bangkok")

name='DataDoanhThu_0930'
prefix='Debt_'
csv_path = '/usr/local/airflow/plugins'+'/'
pk_path = '/usr/local/airflow/plugins/Debt_DataDoanhThuPickles/'
path = '/usr/local/airflow/dags/files/csv_congno/'

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
          schedule_interval= '30 7,9,14 * * *',
          tags=[prefix+name, 'Daily', '60mins']
)

def update_table():
    pass

# transform

def etl_to_postgres():
    if True:
        # day_ago = 3
        datenow = datetime.now().strftime("%Y-%m-%d")
        datenow_7day_ago = ( datetime.now()-timedelta(6) ).strftime("%Y-%m-%d")
        # param_1 = f"'{datenow_day_ago}'"
        Fromdate = datenow_7day_ago
        Todate = datenow
        print(Fromdate, Todate)

        WithOutOrderNbr_sql = \
        f"""
        SELECT co.BranchID,
            COOrDer = co.OrderNbr,
            INOrderNbr = ino.OrderNbr,
            OrigOrderNbr = ino.OrigOrderNbr
        -- INTO #WithOutOrderNbr
        FROM dbo.OM_SalesOrd co WITH (NOLOCK)
            INNER JOIN dbo.OM_SalesOrd ino WITH (NOLOCK)
                ON ino.BranchID = co.BranchID
                AND co.InvcNbr = ino.InvcNbr
                AND co.InvcNote = ino.InvcNote
                AND ino.OrderDate = co.OrderDate
            -- INNER JOIN #TBranchID b WITH (NOLOCK)
                -- ON co.BranchID = b.BranchID
        WHERE co.OrderType IN ( 'CO', 'HK' )
            AND ino.OrderType IN ( 'IN', 'IO', 'EP', 'NP' )
            AND co.Status = 'C'
        """

        WithOutOrderNbr = get_ms_df(WithOutOrderNbr_sql)

        ReturnOrder_sql = \
        """
        SELECT co.BranchID,
            ReturnDate = co.OrderDate,
            ino.OrderDate,
            COOrDer = co.OrderNbr,
            INOrderNbr = ino.OrderNbr,
            OrigOrderNbr = ino.OrigOrderNbr
        -- INTO #ReturnOrder
        FROM dbo.OM_SalesOrd co WITH (NOLOCK)
            INNER JOIN dbo.OM_SalesOrd ino WITH (NOLOCK)
                ON ino.BranchID = co.BranchID
                AND co.InvcNbr = ino.InvcNbr
                AND co.InvcNote = ino.InvcNote
                AND ino.OrderDate <> co.OrderDate
            -- INNER JOIN #TBranchID b WITH (NOLOCK)
            --     ON co.BranchID = b.BranchID
        WHERE co.OrderType IN ( 'CO', 'HK' )
            AND ino.OrderType IN ( 'IN', 'IO', 'EP', 'NP' )
            AND co.Status = 'C'
        """

        ReturnOrder = get_ms_df(ReturnOrder_sql)

        UNION1_sql = \
        f"""
        SELECT ord.BranchID,
                deb.SlsperID,
                ord.OrderNbr,
                OMOrder = sod.OrderNbr,
                DeliveryUnit = '',
                sod.CustID,
                sod.InvoiceCustID,
                do.InvcNbr,
                do.InvcNote,
                sod.Version,
                OrderDate = CAST(do.DocDate AS DATE),
                DateOfOrder = CAST(sod.OrderDate AS DATE),
                DeliveryTime = '',
                TermsID = do.Terms,
                DueDate = CASE
                                WHEN do.Terms = 'O1' THEN
                                    DATEADD(DAY, 30, do.DocDate)
                                ELSE
                                    do.DueDate
                            END,
                OpeiningOrderAmt = do.OrigDocAmt,
                OrdAmtRelease = 0,
                DeliveredOrderAmt = 0,
                ReturnOrdAmt = 0,
                ReceiveAmt = 0,
                Reason = '',
                DebConfirmAmt = 0,
                DebConfirmAmtRelease = 0,
                sod.PaymentsForm
        FROM dbo.OM_PDASalesOrd ord WITH (NOLOCK)
            INNER JOIN dbo.OM_SalesOrd sod WITH (NOLOCK)
                ON sod.BranchID = ord.BranchID
                    AND sod.OrigOrderNbr = ord.OrderNbr
            INNER JOIN dbo.OM_DebtAllocateDet deb
                ON deb.BranchID = ord.BranchID
                    AND deb.OrderNbr = ord.OrderNbr
                    AND sod.ARBatNbr = deb.ARBatNbr
            INNER JOIN dbo.AR_Doc do
                ON sod.BranchID = do.BranchID
                    AND sod.ARBatNbr = do.BatNbr
                    AND sod.ARRefNbr = do.RefNbr
            INNER JOIN Batch b WITH (NOLOCK)
                ON do.BranchID = b.BranchID
                    AND do.BatNbr = b.BatNbr
                    AND b.Module = 'AR'
            -- LEFT JOIN #WithOutOrderNbr woo WITH (NOLOCK)
            --     ON woo.BranchID = ord.BranchID
            --        AND woo.OrigOrderNbr = ord.OrderNbr
            -- LEFT JOIN #TSlsperID ts WITH (NOLOCK)
            --     ON ts.BranchID = deb.BranchID
            --        AND ts.SlsperID = deb.SlsperID
        WHERE CAST(do.DocDate AS DATE)
                BETWEEN '{Fromdate}' AND '{Todate}'
                AND sod.Status = 'C'
            --   AND woo.OrigOrderNbr IS NULL
                AND CAST(sod.OrderDate AS DATE) < '20210501'
        """
        UNION1_p1 = get_ms_df(UNION1_sql)
        UNION1_p0 = pd.read_pickle(pk_path+'UNION1.pk')
        UNION1 = union_all([UNION1_p0, UNION1_p1])

        lst = UNION1.columns
        UNION1 = UNION1.merge(WithOutOrderNbr, how='left', left_on=['BranchID','OrderNbr'], right_on=['BranchID','OrigOrderNbr'])
        # UNION1.columns
        UNION1 = UNION1[UNION1.COOrDer.isna()]
        UNION1 = UNION1[lst]

        # UNION1.shape

        UNION2_sql = \
        f"""
        SELECT
        d.BranchID,
        i.SlsperID,
        d.OrderNbr,
        OMOrder = sod.OrderNbr,
        DeliveryUnit = ISNULL(i.DeliveryUnit, ''),
        sod.CustID,
        sod.InvoiceCustID,
        do.InvcNbr,
        do.InvcNote,
        sod.Version,
        OrderDate = IIF(CAST(i.IssueDate AS DATE) < CAST(sod.OrderDate AS DATE),
                    CAST(sod.OrderDate AS DATE),
                    CAST(i.IssueDate AS DATE)),
        DateOfOrder = CAST(sod.OrderDate AS DATE),
        DeliveryTime = '',
        TermsID = do.Terms,
        DueDate = CASE
                    WHEN do.Terms = 'O1' THEN
                        DATEADD(DAY, 30, do.DocDate)
                    ELSE
                        do.DueDate
                END,
        OpeiningOrderAmt = 0,
        OrdAmtRelease = do.OrigDocAmt,
        DeliveredOrderAmt = 0,
        ReturnOrdAmt = 0,
        ReceiveAmt = 0,
        Reason = '',
        DebConfirmAmt = 0,
        DebConfirmAmtRelease = 0,
        sod.PaymentsForm
        FROM dbo.OM_IssueBook i WITH (NOLOCK)
        INNER JOIN dbo.OM_IssueBookDet d WITH (NOLOCK)
        ON d.BatNbr = i.BatNbr
        AND d.BranchID = i.BranchID
        INNER JOIN dbo.OM_PDASalesOrd ord WITH (NOLOCK)
        ON ord.BranchID = d.BranchID
        AND ord.OrderNbr = d.OrderNbr
        AND ord.OrderType IN ('CO','DI','DM','DP','IN','IR','LO','OO','UP') -- Duy ADDED
        INNER JOIN dbo.OM_SalesOrd sod WITH (NOLOCK)
        ON sod.BranchID = ord.BranchID
        AND sod.OrigOrderNbr = ord.OrderNbr
        -- INNER JOIN #TOrderType ot WITH (NOLOCK)
        --     ON ot.OrderType = ord.OrderType
        INNER JOIN dbo.OM_DebtAllocateDet deb WITH (NOLOCK)
        ON deb.BranchID = d.BranchID
        AND deb.OrderNbr = d.OrderNbr
        AND sod.ARBatNbr = deb.ARBatNbr
        INNER JOIN dbo.AR_Doc do WITH (NOLOCK)
        ON sod.BranchID = do.BranchID
        AND sod.ARBatNbr = do.BatNbr
        AND sod.ARRefNbr = do.RefNbr
        INNER JOIN Batch b WITH (NOLOCK)
        ON do.BranchID = b.BranchID
        AND do.BatNbr = b.BatNbr
        AND b.Module = 'AR'
        -- INNER JOIN #TBranchID br WITH (NOLOCK)
        --     ON i.BranchID = br.BranchID
        -- LEFT JOIN #TSlsperID ts WITH (NOLOCK)
        --     ON ts.BranchID = i.BranchID
        --        AND ts.SlsperID = i.SlsperID
        -- LEFT JOIN #WithOutOrderNbr woo WITH (NOLOCK)
        --     ON woo.BranchID = ord.BranchID
        --        AND woo.OrigOrderNbr = ord.OrderNbr
        WHERE CAST(i.IssueDate AS DATE)
        BETWEEN '{Fromdate}' AND '{Todate}'
        AND i.Status = 'C'
        AND sod.Status = 'C'
        --   AND woo.OrigOrderNbr IS NULL
        """
        
        UNION2_p1 = get_ms_df(UNION2_sql)
        UNION2_p0 = pd.read_pickle(pk_path+'UNION2.pk')
        UNION2 = union_all([UNION2_p0, UNION2_p1])

        lst = UNION2.columns
        UNION2 = UNION2.merge(WithOutOrderNbr, how='left', left_on=['BranchID','OrderNbr'], right_on=['BranchID','OrigOrderNbr'])
        # UNION2.columns
        UNION2 = UNION2[UNION2.COOrDer.isna()]
        UNION2 = UNION2[lst]

        # UNION2.shape

        UNION3_sql = \
        f"""
        SELECT
        b.BranchID,
        b.SlsperID,
        d.OrderNbr,
        OMOrder = sod.OrderNbr,
        DeliveryUnit = ISNULL(b.DeliveryUnit, ''),
        sod.CustID,
        sod.InvoiceCustID,
        do.InvcNbr,
        do.InvcNote,
        sod.Version,
        OrderDate = CAST(dl.LUpd_DateTime AS DATE),
        DateOfOrder = CAST(sod.OrderDate AS DATE),
        DeliveryTime = CAST((DATEDIFF(MINUTE, dl.Crtd_DateTime, dl.LUpd_DateTime)
                            - DATEDIFF(MINUTE, dl.Crtd_DateTime, dl.LUpd_DateTime) % 60
                            ) / 60 AS VARCHAR(8)) + ':'
                        + RIGHT('0' + CAST(DATEDIFF(MINUTE, dl.Crtd_DateTime, dl.LUpd_DateTime) % 60 AS VARCHAR(2)), 2),
        TermsID = do.Terms,
        DueDate = CASE
                        WHEN do.Terms = 'O1' THEN
                            DATEADD(DAY, 30, do.DocDate)
                        ELSE
                            do.DueDate
                    END,
        OpeiningOrderAmt = 0,
        OrdAmtRelease = 0,
        DeliveredOrderAmt = do.OrigDocAmt,
        ReturnOrdAmt = 0,
        ReceiveAmt = 0,
        Reason = '',
        DebConfirmAmt = 0,
        DebConfirmAmtRelease = 0,
        sod.PaymentsForm
        FROM dbo.OM_IssueBook b WITH (NOLOCK)
        INNER JOIN dbo.OM_IssueBookDet d WITH (NOLOCK)
        ON d.BranchID = b.BranchID
            AND d.BatNbr = b.BatNbr
        INNER JOIN dbo.OM_PDASalesOrd o WITH (NOLOCK)
        ON o.BranchID = b.BranchID
        AND o.OrderNbr = d.OrderNbr
        AND o.OrderType IN ('CO','DI','DM','DP','IN','IR','LO','OO','UP') -- Duy ADDED
        INNER JOIN dbo.OM_SalesOrd sod WITH (NOLOCK)
        ON sod.BranchID = o.BranchID
            AND sod.OrigOrderNbr = o.OrderNbr
        -- INNER JOIN #TOrderType ot WITH (NOLOCK)
        -- ON ot.OrderType = o.OrderType
        INNER JOIN dbo.OM_Delivery dl WITH (NOLOCK)
        ON dl.BranchID = d.BranchID
            AND dl.OrderNbr = d.OrderNbr
            AND dl.Status = 'C'
        INNER JOIN dbo.OM_DebtAllocateDet deb WITH (NOLOCK)
        ON deb.BranchID = dl.BranchID
            AND deb.OrderNbr = dl.OrderNbr
            AND deb.ARBatNbr = sod.ARBatNbr
            AND dl.Status = 'C'
        INNER JOIN dbo.AR_Doc do WITH (NOLOCK)
        ON sod.BranchID = do.BranchID
            AND sod.ARBatNbr = do.BatNbr
            AND sod.ARRefNbr = do.RefNbr
        INNER JOIN Batch ba WITH (NOLOCK)
        ON do.BranchID = ba.BranchID
            AND do.BatNbr = ba.BatNbr
            AND ba.Module = 'AR'
        -- INNER JOIN #TBranchID br WITH (NOLOCK)
        -- ON b.BranchID = br.BranchID
        -- LEFT JOIN #TSlsperID ts WITH (NOLOCK)
        -- ON ts.BranchID = b.BranchID
        --     AND ts.SlsperID = b.SlsperID
        -- LEFT JOIN #WithOutOrderNbr woo WITH (NOLOCK)
        -- ON woo.BranchID = o.BranchID
        --     AND woo.OrigOrderNbr = o.OrderNbr
        -- LEFT JOIN #ReturnOrder rod WITH (NOLOCK)
        -- ON rod.BranchID = o.BranchID
        --     AND rod.OrigOrderNbr = o.OrderNbr
        WHERE b.Status = 'C'
        AND CAST(dl.LUpd_DateTime AS DATE)
        BETWEEN '{Fromdate}' AND '{Todate}'
        AND sod.Status = 'C'
        -- AND woo.OrigOrderNbr IS NULL
        -- AND rod.OrigOrderNbr IS NULL
        """

        UNION3_p1 = get_ms_df(UNION3_sql)
        UNION3_p0 = pd.read_pickle(pk_path+'UNION3.pk')
        UNION3 = union_all([UNION3_p0, UNION3_p1])

        lst = UNION3.columns
        UNION3 = UNION3.merge(WithOutOrderNbr, how='left', left_on=['BranchID','OrderNbr'], right_on=['BranchID','OrigOrderNbr'])
        # UNION3.columns
        UNION3 = UNION3[UNION3.OrigOrderNbr.isna()]
        UNION3 = UNION3[lst]

        lst = UNION3.columns
        UNION3 = UNION3.merge(ReturnOrder, how='left', left_on=['BranchID','OrderNbr'], right_on=['BranchID','OrigOrderNbr'], suffixes=['','y'])
        # UNION3.columns
        UNION3 = UNION3[UNION3.OrigOrderNbr.isna()]
        UNION3 = UNION3[lst]

        # UNION3.shape

        UNION4_sql = \
        f"""
        SELECT
        b.BranchID,
        b.SlsperID,
        d.OrderNbr,
        OMOrder = sod.OrderNbr,
        DeliveryUnit = ISNULL(b.DeliveryUnit, ''),
        sod.CustID,
        sod.InvoiceCustID,
        do.InvcNbr,
        do.InvcNote,
        sod.Version,
        OrderDate = CAST(dbc.CreateDate AS DATE),
        DateOfOrder = CAST(sod.OrderDate AS DATE),
        DeliveryTime = '',
        TermsID = do.Terms,
        DueDate = CASE
                        WHEN do.Terms = 'O1' THEN
                            DATEADD(DAY, 30, do.DocDate)
                        ELSE
                            do.DueDate
                    END,
        OpeiningOrderAmt = 0,
        OrdAmtRelease = 0,
        DeliveredOrderAmt = 0,
        ReturnOrdAmt = 0,
        ReceiveAmt = dbc.ReceiveAmt,
        Reason = ISNULL(ors.Descr, ''),
        DebConfirmAmt = 0,
        DebConfirmAmtRelease = 0,
        sod.PaymentsForm
        FROM dbo.OM_IssueBook b WITH (NOLOCK)
        INNER JOIN dbo.OM_IssueBookDet d WITH (NOLOCK)
        ON d.BranchID = b.BranchID
            AND d.BatNbr = b.BatNbr
        INNER JOIN dbo.OM_PDASalesOrd o WITH (NOLOCK)
        ON o.BranchID = b.BranchID
        AND o.OrderNbr = d.OrderNbr
        AND o.OrderType IN ('CO','DI','DM','DP','IN','IR','LO','OO','UP') -- Duy ADDED
        -- LEFT JOIN #ReturnOrder rod WITH (NOLOCK)
        -- ON rod.BranchID = o.BranchID
        --     AND rod.OrigOrderNbr = o.OrderNbr
        -- INNER JOIN #TOrderType ot WITH (NOLOCK)
        -- ON ot.OrderType = o.OrderType
        INNER JOIN dbo.OM_Delivery dl WITH (NOLOCK)
        ON dl.BranchID = d.BranchID
            AND dl.OrderNbr = d.OrderNbr
            AND dl.Status = 'C'
        INNER JOIN dbo.OM_SalesOrd sod WITH (NOLOCK)
        ON sod.BranchID = o.BranchID
        AND sod.OrigOrderNbr = o.OrderNbr
        INNER JOIN dbo.OM_DebtAllocateDet deb WITH (NOLOCK)
        ON deb.BranchID = dl.BranchID
            AND deb.OrderNbr = dl.OrderNbr
            AND dl.Status = 'C'
            AND sod.ARBatNbr = deb.ARBatNbr
        INNER JOIN dbo.AR_Doc do WITH (NOLOCK)
        ON sod.BranchID = do.BranchID
            AND sod.ARBatNbr = do.BatNbr
            AND sod.ARRefNbr = do.RefNbr
        INNER JOIN Batch ba WITH (NOLOCK)
        ON do.BranchID = ba.BranchID
            AND do.BatNbr = ba.BatNbr
            AND ba.Module = 'AR'
        INNER JOIN dbo.PPC_DebtConfirm dbc WITH (NOLOCK)
        ON dbc.InvcNbr = do.InvcNbr
            AND dbc.InvcNote = do.InvcNote
            AND dbc.BranchID = do.BranchID
            AND dbc.DocBatNbr = do.BatNbr
            AND dbc.DocRefNbr = do.RefNbr
        LEFT JOIN OM_ReasonCodePPC ors WITH (NOLOCK)
        ON dbc.Reason = ors.Code
            AND ors.Type = 'DEBTNOTPAY'
        -- INNER JOIN #TBranchID br WITH (NOLOCK)
        -- ON b.BranchID = br.BranchID
        -- LEFT JOIN #TSlsperID ts WITH (NOLOCK)
        -- ON ts.BranchID = b.BranchID
        --     AND ts.SlsperID = b.SlsperID
        -- LEFT JOIN #WithOutOrderNbr woo WITH (NOLOCK)
        -- ON woo.BranchID = o.BranchID
        --     AND woo.OrigOrderNbr = o.OrderNbr
        WHERE b.Status = 'C'
        AND CAST(dbc.CreateDate AS DATE)
        BETWEEN '{Fromdate}' AND '{Todate}'
        AND sod.Status = 'C'
        -- AND woo.OrigOrderNbr IS NULL
        -- AND rod.OrigOrderNbr IS NULL
        """
        
        UNION4_p1 = get_ms_df(UNION4_sql)
        UNION4_p0 = pd.read_pickle(pk_path+'UNION4.pk')
        UNION4 = union_all([UNION4_p0, UNION4_p1])

        lst = UNION4.columns
        UNION4 = UNION4.merge(WithOutOrderNbr, how='left', left_on=['BranchID','OrderNbr'], right_on=['BranchID','OrigOrderNbr'])
        UNION4 = UNION4[UNION4.OrigOrderNbr.isna()]
        UNION4 = UNION4[lst]

        lst = UNION4.columns
        UNION4 = UNION4.merge(ReturnOrder, how='left', left_on=['BranchID','OrderNbr'], right_on=['BranchID','OrigOrderNbr'], suffixes=['','y'])
        UNION4 = UNION4[UNION4.OrigOrderNbr.isna()]
        UNION4 = UNION4[lst]

        # UNION4.shape

        UNION5_sql = \
        f"""
        SELECT 
        d.BranchID,
        SlsperID = ISNULL(ib.SlsperID, deb.SlsperID),
        OrderNbr = d.OrigOrderNbr,
        OMOrder = sod.OrderNbr,
        DeliveryUnit = ISNULL(ib.DeliveryUnit, ''),
        sod.CustID,
        sod.InvoiceCustID,
        do.InvcNbr,
        do.InvcNote,
        sod.Version,
        OrderDate = CAST(d.ReturnDate AS DATE),
        DateOfOrder = CAST(sod.OrderDate AS DATE),
        DeliveryTime = '',
        TermsID = do.Terms,
        DueDate = CASE
                        WHEN do.Terms = 'O1' THEN
                            DATEADD(DAY, 30, do.DocDate)
                        ELSE
                            do.DueDate
                    END,
        OpeiningOrderAmt = 0,
        OrdAmtRelease = 0,
        DeliveredOrderAmt = 0,
        ReturnOrdAmt = do.OrigDocAmt,
        ReceiveAmt = 0,
        Reason = '',
        DebConfirmAmt = 0,
        DebConfirmAmtRelease = 0,
        sod.PaymentsForm
        FROM
        (
        SELECT co.BranchID,
            ReturnDate = co.OrderDate,
            ino.OrderDate,
            COOrDer = co.OrderNbr,
            INOrderNbr = ino.OrderNbr,
            OrigOrderNbr = ino.OrigOrderNbr
        -- INTO #ReturnOrder
        FROM dbo.OM_SalesOrd co WITH (NOLOCK)
            INNER JOIN dbo.OM_SalesOrd ino WITH (NOLOCK)
                ON ino.BranchID = co.BranchID
                AND co.InvcNbr = ino.InvcNbr
                AND co.InvcNote = ino.InvcNote
                AND ino.OrderDate <> co.OrderDate
            -- INNER JOIN #TBranchID b WITH (NOLOCK)
            --     ON co.BranchID = b.BranchID
        WHERE co.OrderType IN ( 'CO', 'HK' )
            AND ino.OrderType IN ( 'IN', 'IO', 'EP', 'NP' )
            AND co.Status = 'C'
        ) as d
        INNER JOIN dbo.OM_PDASalesOrd ord WITH (NOLOCK)
        ON ord.BranchID = d.BranchID
        AND ord.OrderNbr = d.OrigOrderNbr
        AND ord.OrderType IN ('CO','DI','DM','DP','IN','IR','LO','OO','UP') -- Duy ADDED
        INNER JOIN dbo.OM_SalesOrd sod WITH (NOLOCK)
        ON sod.BranchID = ord.BranchID
            AND sod.OrigOrderNbr = ord.OrderNbr
            AND sod.OrderNbr = d.INOrderNbr
        -- INNER JOIN #TOrderType ot WITH (NOLOCK)
        --     ON ot.OrderType = ord.OrderType
        INNER JOIN dbo.OM_DebtAllocateDet deb WITH (NOLOCK)
        ON deb.BranchID = d.BranchID
            AND deb.OrderNbr = d.OrigOrderNbr
            AND deb.ARBatNbr = sod.ARBatNbr
        LEFT JOIN dbo.OM_IssueBookDet bd WITH (NOLOCK)
        ON bd.BranchID = deb.BranchID
            AND bd.OrderNbr = deb.OrderNbr
        LEFT JOIN dbo.OM_IssueBook ib WITH (NOLOCK)
        ON ib.BranchID = bd.BranchID
            AND ib.BatNbr = bd.BatNbr
        INNER JOIN dbo.AR_Doc do WITH (NOLOCK)
        ON sod.BranchID = do.BranchID
            AND sod.ARBatNbr = do.BatNbr
            AND sod.ARRefNbr = do.RefNbr
        INNER JOIN Batch b WITH (NOLOCK)
        ON do.BranchID = b.BranchID
            AND do.BatNbr = b.BatNbr
            AND b.Module = 'AR'
        -- INNER JOIN #TBranchID br WITH (NOLOCK)
        --     ON d.BranchID = br.BranchID
        -- LEFT JOIN #TSlsperID ts WITH (NOLOCK)
        --     ON ts.BranchID = deb.BranchID
        --        AND ts.SlsperID = deb.SlsperID
        -- LEFT JOIN #WithOutOrderNbr woo WITH (NOLOCK)
        --     ON woo.BranchID = ord.BranchID
        --        AND woo.OrigOrderNbr = ord.OrderNbr
        AND sod.Status = 'C'
        --   AND woo.OrigOrderNbr IS NULL
        """
        UNION5 = get_ms_df(UNION5_sql)

        lst = UNION5.columns
        UNION5 = UNION5.merge(WithOutOrderNbr, how='left', left_on=['BranchID','OrderNbr'], right_on=['BranchID','OrigOrderNbr'])
        UNION5 = UNION5[UNION5.OrigOrderNbr.isna()]
        UNION5 = UNION5[lst]

        # UNION5.shape

        UNION6_sql = \
        f"""
        SELECT a.BranchID,
                SlsperID = ISNULL(ib.SlsperID, deb.SlsperID),
                deb.OrderNbr,
                ord.OrderNbr as OrderNbr2,
                OMOrder = ord.OrderNbr,
                DeliveryUnit = ISNULL(ib.DeliveryUnit, ''),
                ord.CustID,
                ord.InvoiceCustID,
                do.InvcNbr,
                do.InvcNote,
                ord.Version,
                OrderDate = CAST(a.Crtd_DateTime AS DATE),
                DateOfOrder = CAST(ord.OrderDate AS DATE),
                DeliveryTime = '',
                TermsID = do.Terms,
                DueDate = CASE
                                WHEN do.Terms = 'O1' THEN
                                    DATEADD(DAY, 30, do.DocDate)
                                ELSE
                                    do.DueDate
                            END,
                --CountOpeningOrder=0,
                OpeiningOrderAmt = 0,
                --CountOrdRelease = 0,
                OrdAmtRelease = 0,
                --CountDelivered = 0,
                DeliveredOrderAmt = 0,
                --CountReturnOrd = 0,
                ReturnOrdAmt = 0,
                ReceiveAmt = 0,
                Reason = '',
                --CountDebtConfirm = 1,
                DebConfirmAmt = CASE
                                    WHEN aDetail.AccountID IS NOT NULL
                                            AND aDetail.AccountID = '711' THEN
                                        0
                                    ELSE
                                        ISNULL(aDetail.Amt, a.AdjAmt)
                                END,
                --CountDebtConfirmRelease = 0,
                DebConfirmAmtRelease = 0,
                ord.PaymentsForm
            --SELECT *
            FROM OM_SalesOrd ord
                INNER JOIN dbo.OM_DebtAllocateDet deb WITH (NOLOCK)
                    ON deb.BranchID = ord.BranchID
                    AND deb.OrderNbr = ord.OrigOrderNbr
                    AND deb.ARBatNbr = ord.ARBatNbr
                LEFT JOIN dbo.OM_IssueBookDet bd WITH (NOLOCK)
                    ON bd.BranchID = deb.BranchID
                    AND bd.OrderNbr = deb.OrderNbr
                LEFT JOIN dbo.OM_IssueBook ib WITH (NOLOCK)
                    ON ib.BranchID = bd.BranchID
                    AND ib.BatNbr = bd.BatNbr
                -- LEFT JOIN #ReturnOrder rto WITH (NOLOCK)
                --     ON rto.BranchID = ord.BranchID
                    --    AND rto.INOrderNbr = ord.OrderNbr
                INNER JOIN dbo.AR_Doc do WITH (NOLOCK)
                    ON ord.BranchID = do.BranchID
                    AND ord.ARBatNbr = do.BatNbr
                    AND ord.ARRefNbr = do.RefNbr
                INNER JOIN dbo.AR_Adjust a WITH (NOLOCK)
                    ON a.AdjdBatNbr = ord.ARBatNbr
                    AND a.AdjdRefNbr = ord.ARRefNbr
                    AND a.BranchID = ord.BranchID
                    AND ISNULL(a.Reversal, '') = ''
                LEFT JOIN dbo.AR_AdjustDetail aDetail WITH (NOLOCK)
                    ON aDetail.BranchID = a.BranchID
                    AND aDetail.BatNbr = a.BatNbr
                    AND aDetail.AdjgBatNbr = a.AdjgBatNbr
                    AND aDetail.AdjgRefNbr = a.AdjgRefNbr
                    AND aDetail.AdjdBatNbr = a.AdjdBatNbr
                    AND aDetail.AdjdRefNbr = a.AdjdRefNbr
                INNER JOIN AR_Doc b WITH (NOLOCK)
                    ON b.BranchID = a.BranchID
                    AND b.BatNbr = a.AdjgBatNbr
                    AND b.RefNbr = a.AdjgRefNbr
                INNER JOIN dbo.Batch ba WITH (NOLOCK)
                    ON a.BranchID = ba.BranchID
                    AND a.BatNbr = ba.BatNbr
                    AND ba.Module = 'AR'
                INNER JOIN Batch bac WITH (NOLOCK)
                    ON do.BranchID = bac.BranchID
                    AND do.BatNbr = bac.BatNbr
                    AND bac.Module = 'AR'
                --  --INNER JOIN dbo.OM_PDASalesOrd sod WITH (NOLOCK)
                --  --    ON sod.BranchID = deb.BranchID
                --  --       AND sod.OrderNbr = deb.OrderNbr
                -- INNER JOIN #TOrderType ot WITH (NOLOCK)
                --     ON ot.OrderType = ord.OrderType
                -- LEFT JOIN #WithOutOrderNbr woo WITH (NOLOCK)
                --     ON woo.BranchID = ord.BranchID
                --        AND woo.OrigOrderNbr = ord.OrigOrderNbr
            WHERE CAST(a.Crtd_DateTime AS DATE)
                BETWEEN '{Fromdate}' AND '{Todate}'
                AND ord.Status = 'C'
                AND ba.Status <> 'V'
                AND ord.OrderType IN ('CO','DI','DM','DP','IN','IR','LO','OO','UP') -- Duy ADDED
                --   AND woo.OrigOrderNbr IS NULL
                --   AND rto.INOrderNbr IS NULL
        """
        
        UNION6_p1 = get_ms_df(UNION6_sql)
        UNION6_p0 = pd.read_pickle(pk_path+'UNION6.pk')
        UNION6 = union_all([UNION6_p0, UNION6_p1])

        lst = UNION6.columns
        UNION6 = UNION6.merge(WithOutOrderNbr, how='left', left_on=['BranchID','OrderNbr'], right_on=['BranchID','OrigOrderNbr'])
        UNION6 = UNION6[UNION6.OrigOrderNbr.isna()]
        UNION6 = UNION6[lst]

        lst = UNION6.columns
        UNION6 = UNION6.merge(ReturnOrder, how='left', left_on=['BranchID','OrderNbr2'], right_on=['BranchID','INOrderNbr'], suffixes=['','y'])
        UNION6 = UNION6[UNION6.INOrderNbr.isna()]
        UNION6 = UNION6[lst]
        drop_cols(UNION6,['OrderNbr2'])

        UNION7_sql = \
        f"""
        SELECT 
        a.BranchID,
        SlsperID = ISNULL(ib.SlsperID, deb.SlsperID),
        sod.OrderNbr,
        ord.OrderNbr as OrderNbr2,
        OMOrder = ord.OrderNbr,
        DeliveryUnit = ISNULL(ib.DeliveryUnit, ''),
        ord.CustID,
        ord.InvoiceCustID,
        do.InvcNbr,
        do.InvcNote,
        ord.Version,
        OrderDate = CAST(a.AdjgDocDate AS DATE),
        DateOfOrder = CAST(ord.OrderDate AS DATE),
        DeliveryTime = '',
        TermsID = do.Terms,
        DueDate = CASE
                        WHEN do.Terms = 'O1' THEN
                            DATEADD(DAY, 30, do.DocDate)
                        ELSE
                            do.DueDate
                    END,
        OpeiningOrderAmt = 0,
        OrdAmtRelease = 0,
        DeliveredOrderAmt = 0,
        ReturnOrdAmt = 0,
        ReceiveAmt = 0,
        Reason = '',
        DebConfirmAmt = 0,
        DebConfirmAmtRelease = CASE
                            WHEN aDetail.AccountID IS NOT NULL
                                AND aDetail.AccountID = '711' THEN
                                0
                            ELSE
                                ISNULL(aDetail.Amt, a.AdjAmt)
                        END,
        ord.PaymentsForm
        FROM OM_SalesOrd ord
        INNER JOIN dbo.OM_DebtAllocateDet deb WITH (NOLOCK)
        ON deb.BranchID = ord.BranchID
            AND deb.OrderNbr = ord.OrigOrderNbr
            AND deb.ARBatNbr = ord.ARBatNbr
        LEFT JOIN dbo.OM_IssueBookDet bd WITH (NOLOCK)
        ON bd.BranchID = deb.BranchID
            AND bd.OrderNbr = deb.OrderNbr
        LEFT JOIN dbo.OM_IssueBook ib WITH (NOLOCK)
        ON ib.BranchID = bd.BranchID
            AND ib.BatNbr = bd.BatNbr
        -- LEFT JOIN #ReturnOrder rto WITH (NOLOCK)
        -- ON rto.BranchID = ord.BranchID
        --     AND rto.INOrderNbr = ord.OrderNbr
        INNER JOIN dbo.AR_Doc do WITH (NOLOCK)
        ON ord.BranchID = do.BranchID
            AND ord.ARBatNbr = do.BatNbr
            AND ord.ARRefNbr = do.RefNbr
        INNER JOIN dbo.AR_Adjust a WITH (NOLOCK)
        ON a.AdjdBatNbr = ord.ARBatNbr
            AND a.AdjdRefNbr = ord.ARRefNbr
            AND a.BranchID = ord.BranchID
            AND ISNULL(a.Reversal, '') = ''
        LEFT JOIN dbo.AR_AdjustDetail aDetail WITH (NOLOCK)
        ON aDetail.BranchID = a.BranchID
            AND aDetail.BatNbr = a.BatNbr
            AND aDetail.AdjgBatNbr = a.AdjgBatNbr
            AND aDetail.AdjgRefNbr = a.AdjgRefNbr
            AND aDetail.AdjdBatNbr = a.AdjdBatNbr
            AND aDetail.AdjdRefNbr = a.AdjdRefNbr
        INNER JOIN AR_Doc b WITH (NOLOCK)
        ON b.BranchID = a.BranchID
            AND b.BatNbr = a.AdjgBatNbr
            AND b.RefNbr = a.AdjgRefNbr
        INNER JOIN dbo.Batch ba WITH (NOLOCK)
        ON a.BranchID = ba.BranchID
            AND a.BatNbr = ba.BatNbr
            AND ba.Module = 'AR'
        INNER JOIN dbo.OM_PDASalesOrd sod WITH (NOLOCK)
        ON sod.BranchID = deb.BranchID
            AND sod.OrderNbr = deb.OrderNbr
        INNER JOIN Batch bac WITH (NOLOCK)
        ON do.BranchID = bac.BranchID
            AND do.BatNbr = bac.BatNbr
            AND bac.Module = 'AR'
        -- INNER JOIN #TOrderType ot WITH (NOLOCK)
        -- ON ot.OrderType = sod.OrderType
        -- INNER JOIN #TBranchID br WITH (NOLOCK)
        -- ON deb.BranchID = br.BranchID
        -- LEFT JOIN #TSlsperID ts WITH (NOLOCK)
        -- ON ts.BranchID = deb.BranchID
        --     AND ts.SlsperID = deb.SlsperID
        -- LEFT JOIN #WithOutOrderNbr woo WITH (NOLOCK)
        -- ON woo.BranchID = sod.BranchID
            -- AND woo.OrigOrderNbr = sod.OrderNbr
        WHERE CAST(a.AdjgDocDate AS DATE)
        BETWEEN '{Fromdate}' AND '{Todate}'
        AND ba.Status = 'C'
        AND ord.Status = 'C'
        AND ord.OrderType IN ('CO','DI','DM','DP','IN','IR','LO','OO','UP') -- Duy ADDED
        -- AND woo.OrigOrderNbr IS NULL
        -- AND rto.INOrderNbr IS NULL
        """
        
        UNION7_p1 = get_ms_df(UNION7_sql)
        UNION7_p0 = pd.read_pickle(pk_path+'UNION7.pk')
        UNION7 = union_all([UNION7_p0, UNION7_p1])

        lst = UNION7.columns
        UNION7 = UNION7.merge(WithOutOrderNbr, how='left', left_on=['BranchID','OrderNbr'], right_on=['BranchID','OrigOrderNbr'])
        UNION7 = UNION7[UNION7.OrigOrderNbr.isna()]
        UNION7 = UNION7[lst]

        lst = UNION7.columns
        UNION7 = UNION7.merge(ReturnOrder, how='left', left_on=['BranchID','OrderNbr2'], right_on=['BranchID','INOrderNbr'], suffixes=['','y'])
        UNION7 = UNION7[UNION7.INOrderNbr.isna()]
        UNION7 = UNION7[lst]
        drop_cols(UNION7,['OrderNbr2'])

        # UNION7.shape

        TOrder = union_all([UNION1,UNION2,UNION3,UNION4,UNION5,UNION6,UNION7])

        # TOrder.shape

        lst = ['BranchID','OrderNbr','OMOrder','PaymentsForm','TermsID','CustID','InvoiceCustID','Version']

        TCustomer = TOrder[lst].copy()

        # vc(TCustomer, 'PaymentsForm')

        TCustomer.PaymentsForm = np.where(TCustomer.PaymentsForm.isin(['B','C']),'TM','CK')

        # vc(TCustomer, 'PaymentsForm')

        c_sql = \
        """
        SELECT 
        CustId as CustID,
        CustName,
        Channel,
        ShopType,
        Territory,
        Addr1,
        Ward,
        District,
        State,
        Attn,
        Phone
        from AR_Customer
        """
        c = get_ms_df(c_sql)

        cl_sql = \
        """
        SELECT 
        Version,
        CustName,
        Channel,
        ShopType,
        Territory,
        Addr1,
        Ward,
        District,
        State,
        Attn,
        Phone
        from AR_HistoryCustClassID
        """
        cl = get_ms_df(cl_sql)

        # TCustomer.shape

        # cl.columns

        TCustomer1 = TCustomer.merge(cl, how = 'left', on=['Version'])

        # TCustomer1.shape

        lst = TCustomer1.columns

        TCustomer2 = TCustomer1.merge(c, how = 'inner', on=['CustID'], suffixes=['','_c'])

        TCustomer2.CustName.fillna(TCustomer2.CustName_c, inplace=True)

        TCustomer2.Channel.fillna(TCustomer2.Channel_c, inplace=True)

        TCustomer2.ShopType.fillna(TCustomer2.ShopType_c, inplace=True)

        TCustomer2.Territory.fillna(TCustomer2.Territory_c, inplace=True)

        TCustomer2.Addr1.fillna(TCustomer2.Addr1_c, inplace=True)

        TCustomer2.Ward.fillna(TCustomer2.Ward_c, inplace=True)

        TCustomer2.District.fillna(TCustomer2.District_c, inplace=True)

        TCustomer2.State.fillna(TCustomer2.State_c, inplace=True)

        TCustomer2.Attn.fillna(TCustomer2.Attn_c, inplace=True)

        TCustomer2.Phone.fillna(TCustomer2.Phone_c, inplace=True)

        TCustomer2 = TCustomer2[lst]

        aci_sql = \
        """
        SELECT
        CustIDInvoice as InvoiceCustID,
        Phone as PhoneCustInvc
        from AR_CustomerInvoice
        """
        aci = get_ms_df(aci_sql)

        TCustomer3 = TCustomer2.merge(aci, how = 'left', on=['InvoiceCustID'])

        # TCustomer3.columns

        TCustomer = TCustomer3

        del(TCustomer1)
        del(TCustomer2)
        del(TCustomer3)

        # TCustomer.shape

        TCustomer.drop_duplicates(inplace=True)

        # TCustomer.shape

        # Output

        o = TOrder
        # del(TOrder)

        tc = TCustomer
        # del(TCustomer)

        sys_sql = """select CpnyID as BranchID, CpnyName from SYS_Company"""
        sys = get_ms_df(sys_sql)

        o.columns

        o = o.merge(sys, how = "inner", on="BranchID")

        u_sql = """
        select Username as SlsperID, FirstName,
        Position =
        CASE
        WHEN u.UserTypes LIKE '%LOG%' THEN'LOG'
        WHEN u.Position IN ( 'D', 'SD', 'AD', 'RD' ) AND u.UserTypes NOT LIKE '%LOG%' THEN 'MDS'
        WHEN u.UserTypes LIKE '%CS%' THEN 'CS' 
        WHEN u.Position IN ( 'S', 'SS', 'AM', 'RM' ) THEN 'P.BH' 
        ELSE u.Position END
        
        from Users as u"""
        u = get_ms_df(u_sql)

        o = o.merge(u, how = "inner", on="SlsperID")

        o = o.merge(tc, how = "inner", on=["BranchID","OMOrder","OrderNbr"], suffixes=['',"_tc"])

        ste_sql = """select Descr as Territory_Name, Territory from SI_Territory"""
        ste = get_ms_df(ste_sql)

        o = o.merge(ste, how = "inner", on=["Territory"], suffixes=['',"_ste"])

        sta_sql = """select Descr as State_Name, State from SI_State"""
        sta = get_ms_df(sta_sql)

        o = o.merge(sta, how = "inner", on=["State"], suffixes=['',"_sta"])

        sd_sql = """select Name as District_Name, District, State from SI_District"""
        sd = get_ms_df(sd_sql)

        o = o.merge(sd, how = "inner", on=["District","State"], suffixes=['',"_sd"])

        sw_sql = """select Name as Ward_Name, Ward, State, District from SI_Ward"""
        sw = get_ms_df(sw_sql)

        o = o.merge(sw, how = "left", on=["Ward", "State","District"], suffixes=['',"_sw"])

        atr_sql = """select Code as DeliveryUnit, Descr as DeliveryUnit_Name from AR_Transporter"""
        atr = get_ms_df(atr_sql)

        o = o.merge(atr, how = "left", on=["DeliveryUnit"], suffixes=['',"_atr"])

        ts_sql = """SELECT SlsperID, BranchID, SupID, ASM, RSMID FROM dbo.fr_ListSaleByData('MR2523');"""
        ts = get_ms_df(ts_sql)

        o = o.merge(ts, how = "left", on=["SlsperID","BranchID"], suffixes=['',"_ts"])

        uu_sql = """SELECT FirstName as SupName, UserName as SupID FROM Users"""
        uu = get_ms_df(uu_sql)

        o = o.merge(uu, how = "left", on=["SupID"], suffixes=['',"_uu"])

        asm_sql = """SELECT FirstName as ASMName, UserName as ASM FROM Users"""
        asm = get_ms_df(asm_sql)

        o = o.merge(asm, how = "left", on=["ASM"], suffixes=['',"_asm"])

        rsm_sql = """SELECT FirstName as RSMName, UserName as RSMID FROM Users"""
        rsm = get_ms_df(rsm_sql)

        o = o.merge(rsm, how = "left", on=["RSMID"], suffixes=['',"_rsm"])

        st_sql = """SELECT DueType, DueIntrv, TermsID as TermsID_tc FROM SI_Terms"""
        st = get_ms_df(st_sql)

        o = o.merge(st, how = "left", on=["TermsID_tc"], suffixes=['',"_st"])

        stt_sql = """SELECT Descr as Terms, TermsID FROM SI_Terms"""
        stt = get_ms_df(stt_sql)

        o = o.merge(stt, how = "left", on=["TermsID"], suffixes=['',"_stt"])

        g = pd.read_csv(path+"gdo.csv")

        o = o.merge(g, how = "left", on=["PaymentsForm_tc","TermsID"], suffixes=['',"_g"])

        o.DebtInCharge.fillna("CS", inplace=True)

        # df = pd.DataFrame(o.columns)

        # df.to_clipboard()

        lst = [
        "BranchID",
        "SlsperID",
        "OrderNbr",
        "CustID",
        "OrderDate",
        "DateOfOrder",
        "DeliveryTime",
        "TermsID",
        "DueDate",
        "CpnyName",
        "FirstName",
        "Position",
        "PaymentsForm_tc",
        "InvoiceCustID_tc",
        "CustName",
        "Channel",
        "ShopType",
        "Addr1",
        "Attn",
        "Phone",
        "Territory_Name",
        "State_Name",
        "District_Name",
        "Ward_Name",
        "DeliveryUnit_Name",
        "SupName",
        "ASMName",
        "RSMName",
        "DueType",
        "DueIntrv",
        "Terms",
        "DebtInCharge",
        "OpeiningOrderAmt",
        "OrdAmtRelease",
        "DeliveredOrderAmt",
        "ReturnOrdAmt",
        "ReceiveAmt",
        "Reason",
        "DebConfirmAmt",
        "DebConfirmAmtRelease",
        ]

        o = o[lst]

        lst = [
        "BranchID",
        "SlsperID",
        "OrderNbr",
        "CustID",
        "OrderDate",
        "DateOfOrder",
        "DeliveryTime",
        "TermsID",
        "DueDate",
        "CpnyName",
        "FirstName",
        "Position",
        "PaymentsForm",
        "InvoiceCustID",
        "CustName",
        "Channel",
        "ShopType",
        "StreetName",
        "Attn",
        "Phone",
        "Territory",
        "State",
        "District",
        "Ward",
        "DeliveryUnit",
        "SupName",
        "ASMName",
        "RSMName",
        "DueType",
        "DueIntrv",
        "Terms",
        "DebtInCharge",
        "OpeiningOrderAmt",
        "OrdAmtRelease",
        "DeliveredOrderAmt",
        "ReturnOrdAmt",
        "ReceiveAmt",
        "Reason",
        "DebConfirmAmt",
        "DebConfirmAmtRelease",
        ]

        o.columns = lst

        # o.dtypes

        dk1 = o.DueType == "D"
        dk2 = o.DueIntrv.isin([1.0, 2.0])

        # vc(o, "DueIntrv")

        o['TermsType'] = np.where(dk1&dk2, "Thanh Toán Ngay", "Cho nợ")

        drop_cols(o, ['DueType','DueIntrv'])

        o.Terms.fillna(o.TermsID,inplace=True)

        drop_cols(o, ['TermsID'])

        # o.columns

        # vc(o,"PaymentsForm")

        wsc = pd.read_csv(path+"wsc.csv")

        # wsc.head()

        o = o.merge(wsc, how = "left", on=["PaymentsForm","ShopType"], suffixes=['',"_wsc"])

        dk = o.No_Count.isna()

        o = o[dk].copy()

        drop_cols(o, "No_Count")

        group_lst = \
        [
        "BranchID",
        "SlsperID",
        "OrderNbr",
        "CustID",
        "OrderDate",
        "DateOfOrder",
        "DueDate",
        "CpnyName",
        "FirstName",
        "Position",
        "PaymentsForm",
        "InvoiceCustID",
        "CustName",
        "Channel",
        "ShopType",
        "StreetName",
        "Attn",
        "Phone",
        "Territory",
        "State",
        "District",
        "Ward",
        "DeliveryUnit",
        "SupName",
        "ASMName",
        "RSMName",
        "Terms",
        "DebtInCharge",
        "TermsType",
        ]

        group_dict = \
        {
        # "DeliveryTime":np.max,
        # "Reason":np.max,
        "OpeiningOrderAmt":np.sum,
        "OrdAmtRelease":np.sum,
        "DeliveredOrderAmt":np.sum,
        "ReturnOrdAmt":np.sum,
        "ReceiveAmt":np.sum,
        "DebConfirmAmt":np.sum,
        "DebConfirmAmtRelease":np.sum
        }

        o = pivot(o, group_lst, group_dict)

        o['DeliveryTime'] = ""
        o['Reason'] = ""

        o["CountOpeningOrder"] = np.where(o.OpeiningOrderAmt>0,1,0)
        o["CountOrdRelease"] = np.where(o.OrdAmtRelease>0,1,0)
        o["DeliveredOrder"] = np.where(o.DeliveredOrderAmt>0,1,0)
        o["CountReturnOrd"] = np.where(o.ReturnOrdAmt>0,1,0)
        o["ConfirmAmt"] = np.where(o.ReceiveAmt>0,1,0)
        o["CountDebtConfirm"] = np.where(o.DebConfirmAmt>0,1,0)
        o["CountDebtConfirmRelease"] = np.where(o.DebConfirmAmtRelease>0,1,0)

        # df = pd.DataFrame(o.columns)
        # df.to_clipboard()

        drop_cols(o, "InvoiceCustID")

        col_lst_rename = \
        [
        "BranchID",
        "SlsperID",
        "OrderNbr",
        "CustID",
        "OrderDate",
        "DateOfOrder",
        "DueDate",
        "BranchName",
        "SlsperName",
        "Position",
        "PaymentsForm",
        "CustName",
        "Channels",
        "SubChannel",
        "Streets",
        "Attn",
        "Phone",
        "Territory",
        "State",
        "District",
        "Ward",
        "DeliveryUnit",
        "SupName",
        "ASMName",
        "RSMName",
        "Terms",
        "DebtInCharge",
        "TermsType",
        "OpeningOrderAmt",
        "OrdAmtRelease",
        "DeliveredOrderAmt",
        "ReturnOrdAmt",
        "ConfirmAmt",
        "DebtConfirmAmt",
        "DebtConfirmAmtRelease",
        "DeliveryTime",
        "ReasonNoPay",
        "CountOpeningOrder",
        "CountOrdRelease",
        "DeliveredOrder",
        "CountReturnOrd",
        "ConfirmAmt",
        "CountDebtConfirm",
        "CountDebtConfirmRelease"
        ]

        o.columns = col_lst_rename

        o.DateOfOrder = pd.to_datetime(o.DateOfOrder, dayfirst=True)
        o.OrderDate = pd.to_datetime(o.OrderDate, dayfirst=True)

        # 23/02/2022

        pn_sql = \
        """
        select DISTINCT
        
        a.OrigOrderNbr,
        a.BranchID,
        b.InvtID
        
        from OM_SalesOrd a
        INNER JOIN OM_SalesOrdDet b ON
        a.BranchID = b.BranchID AND
        a.OrderNbr = b.OrderNbr
        
        INNER JOIN dbo.AR_Doc d ON
        d.BranchID = a.BranchID AND
        d.BatNbr=a.ARBatNbr AND
        a.ARRefNbr=d.RefNbr AND
        d.Terms = 'O1'
        
        WHERE a.OrderType IN ('CO','DI','DM','DP','IN','IR','LO','OO','UP')
        """
        df_pn = get_ms_df(pn_sql)
        sp_lst = ['EH072','EH105','OH016','OH047','OH057','OH071','OH079','OH081']
        dk1 = df_pn['InvtID'].isin(sp_lst)
        df_pn['PN'] = np.where(dk1, "PNM", "MRP")
        
        df_pn = df_pn[['OrigOrderNbr','BranchID','PN']].copy()
        df_pn.drop_duplicates(inplace=True)
        df_pn['count'] = np.where(df_pn['PN']=='PNM', 2 ,1)
        
        df_pn = pivot(df_pn, ['OrigOrderNbr','BranchID'], {'count':np.sum})
        dk1 = df_pn['count'] == 2
        df_pn = df_pn[dk1].copy()
        df_pn.columns = ["OrderNbr","BranchID","count"]
        o = o.merge(df_pn, how='left', on=["OrderNbr","BranchID"]).copy()
        dk1 = o['count'].notna()
        o.Terms = np.where(dk1, "Gối Đầu 30 Pha Nam", o.Terms)
        drop_cols(o, ['count'])
        # End part1


        o['DebtBalance'] = o.OpeningOrderAmt + o.OrdAmtRelease - o.ReturnOrdAmt - o.DebtConfirmAmtRelease

        df1 = o[["BranchID","OrderNbr","DateOfOrder","CustID","Terms","DebtBalance"]].copy()

        # vc(df1, "Terms")

        dk = df1.Terms == "Gối 1 Đơn Hàng (trong 30 ngày)"

        df1 = df1[dk].copy()

        # df1.shape

        df1 = pivot(df1, ["BranchID","OrderNbr","DateOfOrder","CustID"], {"DebtBalance":np.sum})

        ctr1 = df1['DebtBalance'] > 0
        df1 = df1[ctr1].copy()

        df1['DueDate'] = df1.DateOfOrder+timedelta(30)

        df1.sort_values(['DateOfOrder', 'OrderNbr'], axis=0, ascending = (True, True), inplace=True)

        df1a=df1.copy()

        #STEP 1
        df1c = df1a[['BranchID','OrderNbr','DateOfOrder','CustID']].copy()

        # STEP 2
        df1d = pivot(df1c, ['BranchID','DateOfOrder','CustID'], {'OrderNbr':np.min})

        # STEP 3
        df1e = df1c.merge(df1d, how='left', on=['BranchID','DateOfOrder','CustID'], suffixes=['_ori',''])

        del(df1c)
        del(df1d)
        # del(df1e)

        df1a = dropdup(df1a, 1,subset=['BranchID','DateOfOrder','CustID']).copy()

        df1a['group'] = df1a.groupby(['BranchID','CustID'])['DateOfOrder'].diff().gt(pd.to_timedelta('1d'))

        df1a['group'] = df1a.groupby(['BranchID','CustID'])['group'].cumsum()

        df1a['count'] = 1

        df1a['cum_count'] = df1a.groupby(['BranchID','CustID','group'])['count'].cumsum()

        df_ccg = pd.read_csv(path+"cumcountgroup.csv")

        df1a['ccg'] = df1a['cum_count'].map(df_to_dict(df_ccg))

        df1a['group'] = df1a['group'].apply(str) + df1a['ccg']

        drop_cols(df1a, ['count', 'cum_count','ccg'])

        df1a2 = df1a.groupby(['BranchID','CustID', 'group'], as_index=False).agg({'DateOfOrder': np.min})
        df1a = df1a.merge(df1a2, how = 'left', on=['BranchID','CustID','group'], suffixes=('_ori', ''), validate="m:1")
        # drop_cols(df1a, ['group'])
        del(df1a2)

        # df1a.head()

        # df1a.to_clipboard()

        df1b = df1a[['BranchID','CustID', 'DateOfOrder','group']].copy()
        df1b.drop_duplicates(keep='first', inplace=True)
        df1b['cum_count'] = df1b.groupby(['BranchID','CustID']).cumcount()
        drop_cols(df1b, ['DateOfOrder'])

        df1a = df1a.merge(df1b, how='left', on=['BranchID','CustID', 'group'])

        drop_cols(df1a, ['group'])

        df1a['cum_count_plus1'] = df1a['cum_count']+1
        df1b = df1a[['BranchID', 'CustID', 'cum_count', 'DateOfOrder']].drop_duplicates(keep='first')
        df1a = df1a.merge(df1b.add_prefix('y_'), how='left', left_on=['BranchID', 'CustID', 'cum_count_plus1'], right_on=['y_BranchID','y_CustID','y_cum_count'])
        df1a.y_DateOfOrder.fillna(df1a.DueDate, inplace=True)

        df1a.DueDate = np.where(df1a.DueDate>df1a.y_DateOfOrder, df1a.y_DateOfOrder, df1a.DueDate)

        drop_cols(df1a, ['DateOfOrder'])
        df1a.rename(columns={"DateOfOrder_ori": "DateOfOrder"}, inplace=True)

        lst = ['BranchID', 'CustID', 'OrderNbr', 'DateOfOrder', 'DebtBalance', 'DueDate']

        df1a = df1a[lst]

        df1a.drop_duplicates(keep='first', inplace=True)

        # df1b.to_clipboard()

        # End 2302

        # df1a.head()

        df1f = df1e.merge(df1a, how = 'left', on=['BranchID', 'CustID', 'OrderNbr', 'DateOfOrder'])

        drop_cols(df1f, ['OrderNbr'])
        df1f.rename(columns={"OrderNbr_ori": "OrderNbr"}, inplace=True)
        df1f.drop_duplicates(keep='first', inplace=True)

        o = o.merge(df1f, how = "left", on=["BranchID","OrderNbr","DateOfOrder","CustID"], suffixes=['',"_y"])
        drop_cols(o, ['DebtBalance','DebtBalance_y'])
        o.DueDate = np.where(o.DueDate_y.isna(), o.DueDate, o.DueDate_y)
        drop_cols(o, ['DueDate_y'])
        
        dk1 = o.OrderNbr == 'DH122018-17643'
        dk2 = o.BranchID == 'MR0001'
        o.DebtInCharge = np.where(dk1&dk2, "CS", o.DebtInCharge)

        # PART 2
        df1 = o.copy()
 
        df_nhansu = get_bq_df("select * from biteam.d_nhan_su")
        df_nhansu['qlkhuvuc'] = df_nhansu['qlkhuvuc'].str.strip()
        df_nhansu_asm = dropdup(df_nhansu[['manvcrscrss','qlkhuvuc']], 1)
        nhansu_asm_dict = df_to_dict(df_nhansu_asm)
        df_phu_trach_no_cs_theo_tinh = get_bq_df("SELECT * from biteam.d_phu_trach_no_cs_theo_tinh")
        
        phu_trach_no_cs_theo_tinh_dict = df_to_dict(df_phu_trach_no_cs_theo_tinh)
        df1['ASMName'] = np.where(df1['DebtInCharge']=='MDS', df1['SlsperID'].map(nhansu_asm_dict).fillna('Lương Trịnh Thắng (KN)'), df1['State'].map(phu_trach_no_cs_theo_tinh_dict))
        
        ma_kh_cu_dict = get_ms_df("SELECT CustId, RefCustID from AR_Customer").set_index('CustId').to_dict()['RefCustID']
        df1['RefCustId'] = df1['CustID'].map(ma_kh_cu_dict)
        df_phu_trach_no_cs_theo_refcustid = get_bq_df("select * from biteam.d_phu_trach_no_cs_theo_refcustid")
        phu_trach_no_cs_theo_refcustid_dict = get_bq_df("select * from biteam.d_phu_trach_no_cs_theo_refcustid").set_index('refcustid').to_dict()['inchargename']
        list_cs = df_phu_trach_no_cs_theo_refcustid['refcustid'].to_list()
        df1['ASMName_CS'] = df1['RefCustId'].map(phu_trach_no_cs_theo_refcustid_dict)
        df1['ASMName'] = np.where( ((df1['RefCustId'].isin(list_cs)) & (df1['DebtInCharge']=='CS')), df1['ASMName_CS'], df1['ASMName'])
        df_nhansu_sup = dropdup(df_nhansu[['manvcrscrss','quanlytructiep']], 1)
        df_nhansu_sup['quanlytructiep'] = df_nhansu_sup['quanlytructiep'].str.strip()
        # df_to_dict(df_nhansu_sup)
        df1['SupName'] = df1['SupName'].map( df_to_dict(df_nhansu_sup) ).fillna(df1['ASMName'])
        df_nhansu_rsm = dropdup(df_nhansu[['manvcrscrss','qlvung']], 1)
        df_nhansu_rsm['qlvung'] = df_nhansu_rsm['qlvung'].str.strip()
        df1['SupName'] = df1['SupName'].map( df_to_dict(df_nhansu_sup) ).fillna(df1['ASMName'])
        df1['RSMName'] = df1['RSMName'].map( df_to_dict(df_nhansu_rsm) ).fillna(df1['ASMName'])
        
        df1['InChargeName'] =  np.where(df1['DebtInCharge']=='MDS', df1['SlsperName'], df1['ASMName'])
        # vc(df1, 'ASMName')
        df_vptt = get_bq_df("select * from biteam.d_vptt")
        df1 = df1.merge(df_vptt, how = 'left', left_on='State', right_on='tinh',suffixes=('_left', '_right'), validate="m:1")
        df_mkv_viet_tat = get_bq_df("select * from biteam.d_mkv_viet_tat").set_index('tenkhuvuc')
        khuvuc_dict = df_mkv_viet_tat.to_dict()['khuvuc']
        df1['Territory'] = df1['Territory'].map(khuvuc_dict)
        df1['Position'] = np.where(df1['DebtInCharge']=="CS", "CS", df1['Position'])
        
        groupbylist = [
            'OrderNbr',
            'BranchID',
            'Position',
            'SlsperID',
            'SupName',
            'ASMName',
            'RSMName',
            'DateOfOrder',
            'DueDate',
            'CustID',
            'RefCustId',
            'CustName',
            'SlsperName',
            'InChargeName',
            'DebtInCharge',
            'Terms',
            'PaymentsForm',
            'TermsType',
            'Territory',
            'State',
            'vptt',
            'DeliveryUnit',
            'Channels',
            'SubChannel'
            ]
        aggregate_dict = {
        'OrderDate': np.max,
        #Group by tien
        'OpeningOrderAmt':np.sum,
        'OrdAmtRelease':np.sum,
        'DeliveredOrderAmt':np.sum,
        'ReturnOrdAmt': np.sum,
        'DebtConfirmAmt': np.sum,
        'DebtConfirmAmtRelease': np.sum,
        # Huy & Xac Nhan In Month
        # 'ReturnOrdAmt_InMonth': np.sum,
        # 'DebConfirmAmtRelease_InMonth': np.sum,
        
        #DonHang8
        'CountOpeningOrder':np.sum,
        'CountOrdRelease': np.sum,
        'DeliveredOrder':np.sum,
        'CountReturnOrd': np.sum,
        'CountDebtConfirm': np.sum,
        'CountDebtConfirmRelease':np.sum
        }
        
        df2 = pivot(df1, groupbylist, aggregate_dict)
        # df2.columns
        
        rename_dict ={
        # Doanh So
        'OpeningOrderAmt': 'tiennodauky',
        'OrdAmtRelease':'tienchotso',
        'DeliveredOrderAmt':'tiengiaothanhcong',
        'ReturnOrdAmt':'tienhuydon',
        'DebtConfirmAmt':'tienlenbangke',
        'DebtConfirmAmtRelease':'tienthuquyxacnhan',
        # Huy & Xac Nhan In Month
        # 'ReturnOrdAmt_InMonth': 'tienhuydon_inmonth',
        # 'DebConfirmAmtRelease_InMonth': 'tienthuquyxacnhan_inmonth',
        # Don Hang
        'CountOpeningOrder': 'dondauky',
        'CountOrdRelease':'donchotso',
        'DeliveredOrder':'dongiaothanhcong',
        'CountReturnOrd':'donhuy',
        'CountDebtConfirm':'donlenbangke',
        'CountDebtConfirmRelease':'donthuquyxacnhan'
        }
        
        df2.rename(columns=rename_dict, inplace=True)
        # update 04/11
        ctr1 = df2['DeliveryUnit']=='Nhà vận chuyển'
        ctr2 = df2['donhuy']==0
        df2['dongiaothanhcong'] = np.where(ctr1&ctr2, 1, df2['dongiaothanhcong'])
        
        # update 05/11
        ctr1 = df2['DeliveryUnit']=='Nhà vận chuyển'
        ctr2 = df2['tienhuydon']==0
        df2['tiengiaothanhcong'] = np.where(ctr1&ctr2, df2['tienchotso'], df2['tiengiaothanhcong'])
        
        DELI_sql = \
        f"""
        with b as
        (
        SELECT
        BranchID,
        OrderNbr,
        max_se = max(Sequence)
        from OM_Delivery (NOLOCK)
        GROUP BY
        BranchID,
        OrderNbr
        )
        
        SELECT
        a.BranchID,
        a.OrderNbr,
        a.SlsperID as manvgh,
        CASE
        WHEN a.[Status] = 'C' then N'Đã giao hàng'
        WHEN a.[Status] = 'H' then N'Chưa Xác Nhận'
        WHEN a.[Status] = 'D' then N'KH Không Nhận'
        WHEN a.[Status] = 'A' then N'Đã Xác Nhận'
        WHEN a.[Status] = 'R' then N'Từ Chối Giao Hàng'
        WHEN a.[Status] = 'R' then N'Không Tiếp Tục Giao Hàng'
        ELSE N'Chưa Xác Nhận'
        END as trangthaigiaohang,
        a.LUpd_DateTime as deli_last_updated
        from OM_Delivery a
        INNER JOIN b ON
        a.BranchID = b.BranchID AND
        a.OrderNbr = b.OrderNbr AND
        a.Sequence = b.max_se
        """
        
        DELI = get_ms_df(DELI_sql)
        
        df2 = df2.merge(DELI, on = ['BranchID','OrderNbr'], how = 'left')
        df2.deli_last_updated.fillna(datetime(1900,1,1), inplace=True)
        # update 21/02
        dk1 = df2.DeliveryUnit.isna()
        dk2 = df2.DateOfOrder < datetime(2021,5,1)
        df2.trangthaigiaohang = np.where( dk1 & dk2, 'Đã giao hàng', df2.trangthaigiaohang)
        df2.deli_last_updated = np.where( dk1 & dk2, df2.DateOfOrder, df2.deli_last_updated)
        
        dk1 = df2.DeliveryUnit == 'Nhà vận chuyển'
        df2.trangthaigiaohang = np.where( dk1, 'Giao NVC', df2.trangthaigiaohang)
        df2.deli_last_updated = np.where( dk1, df2.DateOfOrder, df2.deli_last_updated)
        
        # end update 19/10
        df2['tiennocongty'] = df2['tiennodauky'] + df2['tienchotso'].values - df2['tienhuydon'].values - df2['tienthuquyxacnhan'].values
        
        df2['donnocongty'] = np.where(df2['tiennocongty'].values > 0, 1, 0)
        df2['donchuagiao'] = df2['donchotso'] - df2['dongiaothanhcong'] - df2['donhuy']
        df2['tiendonchuagiao'] = df2['tienchotso'] - df2['tiengiaothanhcong'] - df2['tienhuydon']
        
        df2[ checkdup(df2, 2, ['OrderNbr', 'BranchID', 'DateOfOrder', 'DueDate']) ].to_csv("CongNo_CheckDup.csv")

        df2.to_pickle(csv_path+f'{prefix}{name}/'+f'{datenow}_'+'F_TrackingDebt_BI.pk')
        # commit_psql("truncate table f_tracking_debt cascade;")
        # execute_values_upsert(df2, 'f_tracking_debt', primary_keys)
        # rows = list(df2.itertuples(index=False, name=None))
    else: print("Not this time then")

def truncate():
    if True:
        commit_psql("truncate table f_tracking_debt cascade;")


def insert():
    datenow = datetime.now().strftime("%Y-%m-%d")
    if True:
        df2 = pd.read_pickle(csv_path+f'{prefix}{name}/'+f'{datenow}_'+'F_TrackingDebt_BI.pk')
        dk = df2.tiennocongty > 1000
        df2 = df2[dk].copy()
        # df2.DateOfOrder = convert_to_datetime(df2.DateOfOrder)
        # df2.DueDate = convert_to_datetime(df2.DueDate)
        # df2.deli_last_updated = pd.to_datetime(df2.deli_last_updated, dayfirst=True)
        
        # dk1=df2['SubChannel'].isin("")
        # dk2=True

        # *------------------*
        #24032022
        df2.columns = lower_col(df2)
        dk1 = df2.subchannel.isin(["NT","PK","SI"])
        dk2 = df2.paymentsform == "TM"
        # dka = df2.terms.isin( ["Gối Đầu 30 Pha Nam","Thu tiền ngay không có VP PN","Thu tiền ngay có VP PN","Gối 1 Đơn Hàng (trong 30 ngày)"] )
        dkfinal1 = dk1&dk2
        dk3 = df2.subchannel == 'CHUOI'
        dk4 = df2.termstype == "Thanh Toán Ngay"
        dk5 = df2.paymentsform == "TM"
        dkfinal2 = dk3&dk4&dk5
        dk6 = df2.subchannel.isin(["INS1","INS2","INS3"])
        df2['debtincharge_v2']=np.where(dkfinal1 | dkfinal2, "MDS", \
            np.where(dk6, "INS", "CS"))
        # *------------------*

        dk1 = df2.ordernbr == 'DH122018-17643'
        dk2 = df2.branchid == 'MR0001'
        df2.debtincharge_v2 = np.where(dk1&dk2, "CS", df2.debtincharge_v2)
        # *------------------*

        #update Hanh input
        dk1 = df2.ordernbr == 'DH062018-13754'
        dk2 = df2.branchid == 'MR0003'
        df2.debtincharge_v2 = np.where(dk1&dk2, "CS", df2.debtincharge_v2)
        #

        df2['inserted_at'] = datetime.now()
        # BQ first 
        bq_values_insert(df2, "f_tracking_debt", 3)
        primary_keys=['ordernbr', 'branchid', 'dateoforder', 'duedate']
        execute_values_upsert(df2, 'f_tracking_debt', primary_keys)



dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

py_update_table = PythonOperator(task_id="update_table", python_callable=update_table, dag=dag)

py_etl_to_postgres = PythonOperator(task_id="etl_to_postgres", python_callable=etl_to_postgres, dag=dag)

truncate = PythonOperator(task_id="truncate", python_callable=truncate, dag=dag, execution_timeout=timedelta(seconds=30))

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)


# tab_refresh = TableauRefreshWorkbookOperator(task_id='tab_refresh', workbook_name='Báo Cáo Doanh Thu Công Nợ', dag=dag)


dummy_start >> py_update_table >> py_etl_to_postgres >> truncate >> insert
# >> tab_refresh
