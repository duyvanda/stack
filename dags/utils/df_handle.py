import pandas as pd
import pandas_gbq as pdq
import numpy as np
import psycopg2
import sys, csv, unidecode, pyodbc, os, time
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects import postgresql
from contextlib import closing
from sqlalchemy import create_engine
from typing import Iterable, List, Optional, Tuple
from datetime import datetime, timedelta
import psycopg2.extras as extras
from psycopg2 import OperationalError, errorcodes, errors
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/usr/local/airflow/dags/files/bigquery1508.json"
pjt = 'spatial-vision-343005'
dts = '.biteam'
# print("DDefault bq project: ",pjt+dts)

def bq_values_insert(df, table:str, option:int, schema=None, reauth=True) -> None:
    '''
    require df, table, option (1-fail, 2-append, 3-replace)
    Schema is a dict, default = None
    '''
    df.columns = cleancols(df)
    df.columns = lower_col(df)
    if option == 1:
        pdq.to_gbq(df, 'biteam.'+table, project_id="spatial-vision-343005", if_exists="fail", reauth=reauth)
    elif option == 2:
        pdq.to_gbq(df, 'biteam.'+table, project_id="spatial-vision-343005", if_exists="append", reauth=reauth)
    elif option == 3:
        pdq.to_gbq(df, 'biteam.'+table, project_id="spatial-vision-343005", if_exists="replace", reauth=reauth)
    else:
        print('option can be 1 2 3')
        raise NameError


def get_bq_client() -> "BQClient":
    '''Return BQClient'''
    return bigquery.Client()

def get_bq_df(sql) -> pd.DataFrame():
    '''
    input a BQ SQL 
    '''
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "D:/spatial-vision-343005-340470c8d77b.json"
    with closing(bigquery.Client()) as client:
        df = client.query(sql).to_dataframe()
        return df

def execute_bq_query(sql:str) -> None:
    with closing(bigquery.Client()) as client:
        client.query(sql)
    

def dfs_diff(*args):
    """
    insert list of df [df1, df2]. No Duplicate in df
    """
    df_list = []
    for arg in args:
        df_list.append(arg)
    return pd.concat(df_list, ignore_index=True).drop_duplicates(keep=False)

def df_filter(df, **kwargs):
    dict  = {}
    for kw, value in kwargs.items():
        dict[f'{kw}'] = value
    return df.loc[(df[list(dict)] == pd.Series(dict)).all(axis=1)]

# https://stackoverflow.com/questions/34157811/filter-a-pandas-dataframe-using-values-from-a-dict
def lo(int):
    list = []
    for i in range(int):
        list.append(f'{i}')
    return list

def vc(df, subset=None):
    return df.value_counts(subset=subset, dropna=False).reset_index(name='counts')

def un(df, str):
    return df[f'{str}'].unique()

def checkna(df):
    return df.isna().sum()

def df_na(df, str):
    return df[df[str].isna()]

def df_notna(df, str):
    return df[~df[str].isna()]

def checkdup(df, option, subset=None):
    """
    return list of True/False
    """
    if option == 1:
        return df.duplicated(subset=subset, keep='first')
    elif option == 2:
        return df.duplicated(subset=subset, keep=False)
    elif option == 3:
        return df.duplicated(subset=subset, keep='last')
    else:
        print('option can be f n l')
        raise NameError

def dropdup(df, option, subset=None):
    """return a dataframe"""
    if option == 1:
        return df.drop_duplicates(subset=subset, keep='first')
    elif option == 2:
        return df.drop_duplicates(subset=subset, keep=False)
    elif option == 3:
        return df.drop_duplicates(subset=subset, keep='last')
    else:
        print('option can be f n l')
        raise NameError

def pivot(df, list, dict):
    return df.groupby(list, dropna=False).aggregate(dict).reset_index()

def cleancols(df):
    """
    return a list
    """
    columns = df.columns
    columns_list = []
    for c in columns:
        c = unidecode.unidecode(c)
        columns_list.append(c)
    df.columns = columns_list
    df.columns = df.columns.str.replace(' ', '', regex=False)
    df.columns = df.columns.str.replace('/', '', regex=False)
    df.columns = df.columns.str.replace('(', '', regex=False)
    df.columns = df.columns.str.replace(')', '', regex=False)
    df.columns = df.columns.str.replace(':', '', regex=False)
    df.columns = df.columns.str.replace('.', '', regex=False)
    df.columns = df.columns.str.replace('+', '', regex=False)
    df.columns = df.columns.str.replace('\n', '', regex=False)
    df.columns = df.columns.str.replace('-', '', regex=False)
    df.columns = df.columns.str.replace('*', '', regex=False)
    df.columns = df.columns.str.replace('&', '', regex=False)
    return df.columns

def print_df_schema(df):
    psqltypes={'datetime64[ns]':'TIMESTAMP', 'int64':'BIGINT', 'int32':'BIGINT',  'float64':'NUMERIC(24)',   'object':'VARCHAR(100)', 'bool':'BOOLEAN'}
    dftype = []
    for i in df.dtypes:
        dftype.append(str(i))
    dftype = list((pd.Series(dftype)).map(psqltypes))
    for col, ty in zip(df.columns, dftype):
        print(col, str(ty)+" NULL,")

postgres_conf = {"host": "171.235.26.161","port": 5432,"user": "biteam","password": "123biteam","database": "biteam"}

def get_postgres_engine(postgres_conf):
    engine = create_engine(
        "postgresql://{user}:{pw}@{host}:{port}/{db}".format(
            user=postgres_conf["user"],
            pw=postgres_conf["password"],
            host=postgres_conf["host"],
            port=postgres_conf["port"],
            db=postgres_conf["database"],
        )
    )
    return engine

def get_pg2_conn():
    return psycopg2.connect(**postgres_conf)

def insert_df_to_postgres(tbl_name, df, primary_keys: list, schema="biteam"):
    if df.shape[0] < 1:
        return

    def norm_value(d: dict):
        for k, v in d.items():
            if isinstance(v, (list, tuple)):
                continue
            if pd.isnull(v):
                d[k] = None
        return d

    engine = get_postgres_engine(postgres_conf)
    meta = MetaData(schema=schema)

    x = [x.lower() for x in df.columns]
    df.columns = x

    conn = engine.connect()
    my_table = Table(tbl_name, meta, autoload=True, autoload_with=conn)
    insert_statement = postgresql.insert(my_table).values(
        list(map(norm_value, df.to_dict(orient="records")))
    )
    if len(primary_keys):
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=primary_keys,
            set_={
                c.key: c for c in insert_statement.excluded if c.key not in primary_keys
            },
        )
        conn.execute(upsert_statement)
        conn.close()
    else:
        conn.execute(insert_statement)
        conn.close()

engine = create_engine('postgresql+psycopg2://biteam:123biteam@171.235.26.161/biteam')

def get_conn():
    return engine.connect()

def get_cur():
    return engine.raw_connection().cursor()

def get_ps_df(sql):
        """
        Executes the sql and returns a pandas dataframe

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        if sys.version_info[0] < 3:
            sql = sql.encode('utf-8')
        import pandas.io.sql as psql

        with get_conn() as conn:
            return psql.read_sql(sql, con=conn)

def get_records(sql):
        """
        Executes the sql and returns a set of records.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        if sys.version_info[0] < 3:
            sql = sql.encode('utf-8')
        with closing(get_cur()) as cur:
            if parameters is not None:
                cur.execute(sql, parameters)
            else:
                cur.execute(sql)
            return cur.fetchall()


def get_ms_df(sql, parse_dates=None):
    server = '115.165.164.234'
    driver = 'SQL Server'
    db1 = 'PhaNam_eSales_PRO'
    tcon = 'no'
    uname = 'duyvq'
    pword = '123VanQuangDuy'
    with closing(pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}', host=server, database=db1, trusted_connection=tcon, user=uname, password=pword)) as cnxn:
        df = pd.read_sql(sql, cnxn, parse_dates=parse_dates)
        return df

def df_to_dict(df):
    """
    insert df with 2 columns
    """
    dict = df.set_index(df.columns[0]).to_dict()[df.columns[1]]
    return dict

def get_ms_csv(sql, path):
    server = '115.165.164.234'
    db1 = 'PhaNam_eSales_PRO'
    tcon = 'no'
    uname = 'duyvq'
    pword = '123VanQuangDuy'
    with closing(pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}', host=server, database=db1, trusted_connection=tcon, user=uname, password=pword)) as cnxn:
        with closing(cnxn.cursor()) as cursor:
            data = cursor.execute(sql).fetchall()
            with open(path, 'w', newline='', encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([x[0] for x in cursor.description])
                for row in data:
                    writer.writerow(row)

# Update 30/10/2021
def commit_ms_sql(sql):
    server = '115.165.164.234'
    db1 = 'PhaNam_eSales_PRO'
    tcon = 'no'
    uname = 'duyvq'
    pword = '123VanQuangDuy'
    with closing(pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}', host=server, database=db1, trusted_connection=tcon, user=uname, password=pword)) as cnxn:
        with closing(cnxn.cursor()) as cursor:
            cursor.execute(sql)
        cnxn.commit()

def insert_to_ms_and_update(df, str, update_sql):
    server = '115.165.164.234'
    tcon = 'no'
    database = 'PhaNam_eSales_PRO'
    driver = "ODBC Driver 17 for SQL Server"
    uname = 'duyvq'
    pword = '123VanQuangDuy'
    engine = create_engine(f"mssql+pyodbc://{uname}:{pword}@{server}/{database}?driver={driver}?Trusted_Connection={tcon}", fast_executemany = True)
    with closing(engine.connect()) as cnxn:
        cnxn.execute(f"""DROP TABLE IF EXISTS {str};""")
        # cnxn.execute(sql_create)
        df.to_sql(str, con = cnxn, index = False, if_exists = 'replace')
        cnxn.execute(update_sql)
        cnxn.execute(f"""DROP TABLE {str};""")




# AIRFLOW INSERT
def generate_insert_psql(table: str, values: Tuple[str, ...], target_fields: Iterable[str], replace: bool, **kwargs) -> str:
    """
    Static helper method that generate the INSERT SQL statement.
    :param table: Name of the target table
    :type table: str
    :param values: The row to insert into the table
    :type values: tuple of cell values
    :param target_fields: The names of the columns to fill in the table
    :type target_fields: iterable of strings
    :param replace: Whether to replace instead of insert
    :type replace: bool
    :param replace_index: the column or list of column names to act as
        index for the ON CONFLICT clause
    :type replace_index: str or list
    :return: The generated INSERT or REPLACE SQL statement
    :rtype: str
    """
    placeholders = [
        "%s",
    ] * len(values)
    replace_index = kwargs.get("replace_index")

    if target_fields:
        target_fields_fragment = ", ".join(target_fields)
        target_fields_fragment = f"({target_fields_fragment})"
    else:
        target_fields_fragment = ''

    sql = f"INSERT INTO {table} {target_fields_fragment} VALUES ({','.join(placeholders)})"

    if replace:
        if target_fields is None:
            raise ValueError("PostgreSQL ON CONFLICT upsert syntax requires column names")
        if replace_index is None:
            raise ValueError("PostgreSQL ON CONFLICT upsert syntax requires an unique index")
        if isinstance(replace_index, str):
            replace_index = [replace_index]
        replace_index_set = set(replace_index)

        replace_target = [
            "{0} = excluded.{0}".format(col) for col in target_fields if col not in replace_index_set
        ]
        sql += f" ON CONFLICT ({', '.join(replace_index)}) DO UPDATE SET {', '.join(replace_target)}"
        # print(sql)
    return sql

def serialize_cell(cell: object, conn) -> object:
    """
    Postgresql will adapt all arguments to the execute() method internally,
    hence we return cell without any conversion.
    See http://initd.org/psycopg/docs/advanced.html#adapting-new-types for
    more information.
    :param cell: The cell to insert into the table
    :type cell: object
    :param conn: The database connection
    :type conn: connection object
    :return: The cell
    :rtype: object
    """
    return cell

def insert_rows_psql(table, rows, target_fields=None, commit_every=1000, replace=True, **kwargs):
    """
    A generic way to insert a set of tuples into a table,
    a new transaction is created every commit_every rows

    :param table: Name of the target table
    :type table: str
    :param rows: The rows to insert into the table
    :type rows: iterable of tuples
    :param target_fields: The names of the columns to fill in the table
    :type target_fields: iterable of strings
    :param commit_every: The maximum number of rows to insert in one
    transaction. Set to 0 to insert all rows in one transaction.
    :type commit_every: int
    :param replace: Whether to replace instead of insert
    :type replace: bool
    """
    i = 0
    with closing(get_pg2_conn()) as conn:
        with closing(conn.cursor()) as cur:
            for i, row in enumerate(rows, 1):
                lst = []
                for cell in row:
                    lst.append(serialize_cell(cell, conn))
                values = tuple(lst)
                sql = generate_insert_psql(table, values, target_fields, replace, **kwargs)
                cur.execute(sql, values)
                if commit_every and i % commit_every == 0:
                    conn.commit()
                    print("Loaded %s rows into %s so far", i, table)

        conn.commit()
    print("Done loading. Loaded a total of %s rows", i)

# Update 02/11
def commit_psql(sql):
    """
    input sql
    """
    with closing(get_pg2_conn()) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(sql)            
        conn.commit()

# Define function using psycopg2.extras.execute_values() to insert the dataframe.
def execute_values_upsert(datafrm, table, pk):
    # Creating a list of tupples from the dataframe values
    conn = get_pg2_conn()
    tpls = [tuple(x) for x in datafrm.to_numpy()]
    # dataframe columns with Comma-separated
    cols = ','.join(list(datafrm.columns)) 
    # SQL query to execute
    sql = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    replace_target = ["{0} = excluded.{0}".format(col) for col in datafrm.columns if col not in pk]
    strr = f" ON CONFLICT ({', '.join(pk)}) DO UPDATE SET {', '.join(replace_target)}"
    sql += strr
    print(sql)
    cursor = conn.cursor()
    # try:
    extras.execute_values(cursor, sql, tpls, page_size=100)
    conn.commit()
    print("Data inserted using execute_values() successfully..")
    conn.close()
    cursor.close()
    # except (Exception, psycopg2.DatabaseError) as err:
    #     print(sql)
    #     print(err)
    #     conn.close()
    #     cursor.close()

# Define function using psycopg2.extras.execute_values() to insert the dataframe.
def execute_values_insert(datafrm, table):
    conn = get_pg2_conn()
    # Creating a list of tupples from the dataframe values
    tpls = [tuple(x) for x in datafrm.to_numpy()]
    # dataframe columns with Comma-separated
    cols = ','.join(list(datafrm.columns))
    # SQL query to execute
    sql = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, sql, tpls)
        conn.commit()
        print("Data inserted using execute_values() successfully..")
    except (Exception, psycopg2.DatabaseError) as err:
        # pass exception to function
        print(err)
        cursor.close()

# Update 24/10    
def convert_to_datetime(series):
    """
    series=pd.to_datetime(series.str[:10], dayfirst=True)
    """
    series=pd.to_datetime(series.str[:10], dayfirst=True)
    return series

# Update 24/10
def date_between_handler(df, ctr1, ctr2, str, list_check):
    """
    THIS FUNCTION IS FOR LEFT JOIN WITH DATE CONDITION LIKE
        LEFT JOIN #T_ExtRoute et WITH (NOLOCK) ON 
        pso.BranchID = et.BranchID AND
        CAST(pso.Crtd_DateTime AS DATE) BETWEEN et.StartDate AND et.EndDate
    df = data frame
    ctr1, ctr2 = predefined, example: ctr1=df.Created_Date<df.df2_StartDate; ctr2=df1.Created_Date>df1.df2_EndDate
    series = df series should convert to np.NaN
    list_check = key joins + date columns (Created_Date) in bewteen (StartDate, EndDate): ['brand', 'city', 'Created_Date']

    FULL EXAMPLES:
    ctr1=df3.Created_Date<df3.df2_StartDate
    ctr2=df3.Created_Date>df3.df2_EndDate
    series = df3['df2_brand']
    list_check = ['brand', 'city', 'Created_Date']
    df3 = date_between_handler(df3,ctr1,ctr2,series,list_check)
    """
    df['ERR']=np.where(ctr1|ctr2, True,False)
    print(f"row errors = {df['ERR'].sum()}")
    df[f'{str}'] = np.where(df['ERR'], np.NaN, df[f'{str}'])
    # Chia df ra lam 2 phan a) Phan Loi b) Phan khong loi
    dfb = df[df['ERR']]
    dfa = df[~df['ERR']]
    # Check các dòng nằm trong phần lỗi có nằm trong phần không lỗi
    dfb = pd.merge(dfb, dfa[list_check], how='left',  on=list_check, indicator=True)
    # Chỉ giữ lại các dòng không nằm trong phần không lỗi
    ctr1 = dfb['_merge']=='left_only'
    dfb = dfb[ctr1]
    # Loại bỏ các dòng lỗi nhưng bị dup va cac dong khong can thiet
    dfb = dropdup(dfb,1,list_check).drop(labels=['ERR','_merge'], axis=1)
    print(f"row errors keep = {dfb.shape[0]}")
    print()
    df = dfa.append(dfb).drop(labels=['ERR'], axis=1)
    return df

# Update 25/10
def union(list):
    """
    input 2 dfs as a list [df1,df2]
    """
    return pd.concat(list,ignore_index=True).drop_duplicates().reset_index(drop=True)

def union_all(list):
    """
    input 2 dfs as a list [df1,df2]
    """
    return pd.concat(list, ignore_index=True)

def lower_col(df):
    return [x.lower() for x in df.columns]

def drop_cols(df, list):
    """
    df: dataframe
    list: labels list
    """
    df.drop(labels=list, axis=1, inplace=True)
    
