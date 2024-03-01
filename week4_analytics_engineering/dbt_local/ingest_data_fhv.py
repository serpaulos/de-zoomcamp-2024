import io
import os
from dotenv import load_dotenv

import requests
from google.cloud import storage
from pathlib import Path

load_dotenv()

BUCKET = os.getenv("GCP_GCS_BUCKET", "GCP_GCS_BUCKET")
os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_APPLICATION_CREDENTIALS") 


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

#https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz
def download(year, service):
    
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]


        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{service}_tripdata_{year}-{month}.csv.gz"

        if not os.path.exists(f"data/{service}"):
            os.makedirs(f"data/{service}") 

        # Chunk size for downloading
        chunk_size = 1024 * 1024  # 1 MB chunks

        # downloading the file sending the request to URL
        req = requests.get(url, stream=True)

        # Determine the total file size from the Content-Length header
        total_size = int(req.headers.get('content-length', 0))

        data_folder = f"data/{service}"

        # split url to get file name
        file_name = url.split('/')[-1]
        dest_folder = data_folder
        file_path = os.path.join(dest_folder, file_name)


        with open(file_path, 'wb') as file:
            for chunk in req.iter_content(chunk_size):
                if chunk:
                    file.write(chunk)
                    file_size = file.tell()
                    print(f'Downloading {file_name} ... {file_size}/{total_size} bytes', end='\r')
        
        taxi_dtypes = {
            'dispatching_base_num': str,
            'PUlocationID': pd.Int64Dtype(),
            'DOlocationID': pd.Int64Dtype(),
            'SR_Flag': str,
            'Affiliated_base_number': str,
        }
        parse_dates = ['pickup_datetime', 'dropOff_datetime']

        new_csv = pd.read_csv(file_path, compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
        #print(new_csv.dtypes)
        new_csv = new_csv.to_csv(file_path, compression='gzip')
        #print(new_file_name, file_path)

        #upload it to gcs 
        upload_to_gcs(BUCKET, file_path, file_path)
        print(f"GCS: {file_path}")

download('2019', 'fhv')
# download('2020', 'green')
# download('2019', 'yellow')
# download('2020', 'yellow')
