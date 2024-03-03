import os

import requests


def download(year, service):
    for i in range(12):
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{service}_tripdata_{year}-{month}.csv.gz"

        if not os.path.exists(f"./data/raw/{service}/{year}/{month}"):
            os.makedirs(f"./data/raw/{service}/{year}/{month}") 

        # Chunk size for downloading
        chunk_size = 1024 * 1024  # 1 MB chunks

        # downloading the file sending the request to URL
        req = requests.get(url, stream=True)

        # Determine the total file size from the Content-Length header
        total_size = int(req.headers.get('content-length', 0))
        
        data_folder = f"./data/raw/{service}/{year}/{month}"

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

download(2020, "yellow")
download(2020, "green")