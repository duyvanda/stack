import io
import shutil
from google.oauth2 import service_account
from googleapiclient import discovery
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

def get_file(filepath):
    """
    filepath: example /usr/local/airflow/plugins/nhan/data.xlsx
    """
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/drive.file']
    jsonfile = '/usr/local/airflow/plugins/nhan/datateam1599968716114-6f9f144b4262.json'
    credentials = service_account.Credentials.from_service_account_file(jsonfile, scopes=scopes)
    service = discovery.build('drive', 'v3', credentials=credentials)

    # folderid_excel = '1m54gDqWhNz-D0LU4In75lvaRd5OwBG5E'
    # query = f"parents = '{folderid_excel}'"
    # files = service.files().list(q=query).execute()

    file_id = '1EmMS46usylDzxDrKzteFQk2XzlZEMGpD'

    fh = io.BytesIO()

    request = service.files().get_media(fileId=file_id)

    downloader = MediaIoBaseDownload(fh, request, chunksize=204800)

    done = False

    while not done:
        status, done = downloader.next_chunk()

    fh.seek(0)
    with open(filepath, 'wb') as f:
                    shutil.copyfileobj(fh, f)

# df =  pd.read_excel("duy.xlsx")

# df.head()

