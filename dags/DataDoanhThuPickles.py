from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.tableau.operators.tableau_refresh_workbook import TableauRefreshWorkbookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


local_tz = pendulum.timezone("Asia/Bangkok")

name='DataDoanhThuPickles'
prefix='Debt_'
csv_path = '/usr/local/airflow/plugins/Debt_DataDoanhThuPickles/'
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
          schedule_interval= '@daily',
          tags=[prefix+name, '@daily']
)

def update_table():
    pass

# transform

def get_pickles():
    if True:
        # day_ago = 3
        datenow = datetime.now().strftime("%Y-%m-%d")
        datenow_7day_ago = ( datetime.now()-timedelta(7) ).strftime("%Y-%m-%d")
        # param_1 = f"'{datenow_day_ago}'"
        Fromdate = '2016-01-01'
        Todate = datenow_7day_ago
        print(Fromdate, Todate)

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
        UNION1 = get_ms_df(UNION1_sql)

        UNION1.to_pickle(csv_path+'UNION1.pk')

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
        UNION2 = get_ms_df(UNION2_sql)

        UNION2.to_pickle(csv_path+'UNION2.pk')

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
        UNION3 = get_ms_df(UNION3_sql)

        UNION3.to_pickle(csv_path+'UNION3.pk')

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
        UNION4 = get_ms_df(UNION4_sql)

        UNION4.to_pickle(csv_path+'UNION4.pk')

        # UNION4.shape

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
                DebConfirmAmt = ISNULL(a.AdjAmt,0),           
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
        UNION6 = get_ms_df(UNION6_sql)

        UNION6.to_pickle(csv_path+'UNION6.pk')

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
        DebConfirmAmtRelease = ISNULL(a.AdjAmt,0),
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
        UNION7 = get_ms_df(UNION7_sql)

        UNION7.to_pickle(csv_path+'UNION7.pk')

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

get_pickles = PythonOperator(task_id="get_pickles", python_callable=get_pickles, dag=dag )

# insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

# insert_ins = PythonOperator(task_id="insert_ins", python_callable=insert_ins, dag=dag)



# tab_refresh = TableauRefreshWorkbookOperator(task_id='tab_refresh', workbook_name='Báo Cáo Doanh Thu Công Nợ', dag=dag)


dummy_start >> get_pickles
# >> tab_refresh
