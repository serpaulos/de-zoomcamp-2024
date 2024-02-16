import io
import os
import requests
import pandas as pd

def web_to_gcs(year, service, save_dir):


    # services = ['fhv','green','yellow']
    init_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'


    if not os.path.exists(save_dir):
        os.makedirs(save_dir)


    for i in range(12):
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        #https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet
        file_name = f"{save_directory}/{service}_tripdata_{year}-{month}.parquet"

        # download it using requests via a pandas df
        request_url = f"{init_url}/{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {request_url}")

save_directory = 'week3_data_warehouse_bq/homework/'
web_to_gcs('2022', 'yellow', save_directory)
#web_to_gcs('2020', 'green')
# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')

