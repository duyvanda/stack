from google.oauth2 import service_account
from googleapiclient import discovery

def get_service():
    scopes = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/drive.file']
    jsonfile = '/usr/local/airflow/plugins/nhan/datateam1599968716114-6f9f144b4262.json'
    credentials = service_account.Credentials.from_service_account_file(jsonfile, scopes = scopes)
    service = discovery.build('sheets','v4',credentials = credentials)
    return service

