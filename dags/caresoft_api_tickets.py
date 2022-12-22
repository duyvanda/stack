from utils.df_handle import *
import requests
from vars import vars

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Bangkok")
name='API_TICKET'
prefix='CARESOFT_'
path = f'/usr/local/airflow/plugins/{prefix}{name}/'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2022, 4, 5, tzinfo=local_tz),
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
          # schedule_interval= '10 0 * * *',
          schedule_interval= '@once',
          tags=[prefix+name]
)

base_url = 'https://api.caresoft.vn/merapgroup/api/v1/'
headers = {"Authorization": f"Bearer {vars['name']}"}

datenow_min1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
def insert_to_ps():
    list_pages=[1,2,3,4,5,6,7,8,9,10]
    for page in list_pages:
        extra_url=f'tickets?updated_since={datenow_min1}T00:00:00Z&conversation_type=3&page={page}&count=500'
        r = requests.get(base_url+extra_url, headers=headers)
        # r.json()
        data =  r.json()['tickets']
        print("do dai cua data: ",len(data))
        df1=pd.DataFrame.from_records(data)
        try:
          assert(df1.shape[0] > 0)
        except AssertionError:
          print("Khong co data")
          break
        else:
          dk1 = df1.assignee_id.notna()
          df1 = df1[dk1].copy()
          print("so dong ticket la: "+ str(df1.shape[0]))
          print("Else Branch")
          assignee_df = pd.DataFrame(df1['assignee'].tolist(), index=df1.index)
          requester_df = pd.DataFrame(df1['requester'].tolist(), index=df1.index)
          assignee_df.columns = ["assignee_"+ e for e in assignee_df.columns]
          requester_df.columns = ["requester_"+ e for e in requester_df.columns]
          # assignee_df.columns
          drop_cols(df1, ['assignee_id','requester_id'])
          df1 = pd.concat([df1,assignee_df], axis=1).copy()
          df1 = pd.concat([df1,requester_df], axis=1).copy()
          drop_cols(df1, ['assignee','requester'])
          dk1 = df1.ticket_subject.str.split().str.get(0) == 'Định'
          dk2 = df1.ticket_subject.str.split().str.get(0) == 'Chat'
          df1['ticket_subject_type'] = \
              np.where(dk1, "D", \
              np.where(dk2, "C","O"))
          # df1[['ticket_subject','ticket_subject_type']].to_clipboard()
          # df1.dtypes
          df1.created_at = pd.to_datetime(df1.created_at, dayfirst=True)
          df1.updated_at = pd.to_datetime(df1.updated_at, dayfirst=True)
          df1.created_at.fillna(datetime(1900,1,1), inplace=True)
          df1.updated_at.fillna(datetime(1900,1,1), inplace=True)
          # df1.is_overdue.isna()
          df1.is_overdue = df1.is_overdue.astype(str).str.replace('nan', '')
          df1.ticket_source_end_status = df1.ticket_source_end_status.astype(str).str.replace('nan', '')
          # df1.assignee_group_id.isna().sum()
          df1.to_csv(path+"TICKETS_"+f"{datenow_min1}.csv", index=False)
          drop_cols(df1, ['custom_fields', 'tags', 'ccs','follows','requester_organization_id'])
          execute_values_upsert(df1, "f_caresoft_tickets", pk=['ticket_id'])
          print("page vua insert "+str(page))

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

insert_to_ps = PythonOperator(task_id="insert_to_ps", python_callable=insert_to_ps, dag=dag)

dummy_start >> insert_to_ps