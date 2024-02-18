CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-2024-411615.de_zoomcamp_2024_ny_taxi.external_yellow_tip_data`
(
VendorID integer,
tpep_pickup_datetime datetime,
tpep_dropoff_datetime datetime,
passenger_count integer,
trip_distance float64,
RatecodeID integer,
store_and_fwd_flag string,
PULocationID integer,
DOLocationID integer,
payment_type integer,
fare_amount float64,
extra float64,
mta_tax float64,
tip_amount float64,
tolls_amount float64,
improvement_surcharge float64,
total_amount float64,
congestion_surcharge float64
)
OPTIONS (
  format = 'parquet',
  uris = ['gs://zoomcamp_2004_bucket/ny_taxi_data.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-2024-411615.de_zoomcamp_2024_ny_taxi.external_yellow_tip_data`
OPTIONS (
  format = 'CSV',
  uris = ['gs://zoomcamp_2004_bucket/yellow_tripdata_2021-01.csv.gz']
);

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-2024-411615.de_zoomcamp_2024_ny_taxi.tripdata_all`
OPTIONS (
  format = 'parquet',
  uris = ['gs://zoomcamp_2004_bucket/green_tripdata_2022-*.parquet', 'gs://zoomcamp_2004_bucket/yellow_tripdata_2022-*.parquet']
);

CREATE TABLE de-zoomcamp-2024-411615.trip_data_all.green_tripdata AS
SELECT * FROM bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2019;

CREATE TABLE de-zoomcamp-2024-411615.trip_data_all.yellow_tripdata AS
SELECT * FROM bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2019;

insert into de-zoomcamp-2024-411615.trip_data_all.green_tripdata
SELECT * FROM bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2020;

insert into de-zoomcamp-2024-411615.trip_data_all.yellow_tripdata
SELECT * FROM bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2020;


CREATE OR REPLACE EXTERNAL TABLE  `de-zoomcamp-2024-411615.trips_data_all.yellow_trip_data`
(
VendorID integer,
tpep_pickup_datetime datetime,
tpep_dropoff_datetime datetime,
passenger_count integer,
trip_distance float64,
RatecodeID integer,
store_and_fwd_flag string,
PULocationID integer,
DOLocationID integer,
payment_type integer,
fare_amount float64,
extra float64,
mta_tax float64,
tip_amount float64,
tolls_amount float64,
improvement_surcharge float64,
total_amount float64,
congestion_surcharge float64
)
OPTIONS (
  format='parquet',
  uris = ['gs://psergios_zoomcamp_2024/yellow/yellow_tripdata_2019-*.parquet', 'gs://psergios_zoomcamp_2024/yellow/yellow_tripdata_2020-*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE  `de-zoomcamp-2024-411615.trips_data_all.green_trip_data`
(
VendorID integer,
lpep_pickup_datetime datetime,
lpep_dropoff_datetime datetime,
passenger_count integer,
trip_distance float64,
RatecodeID integer,
store_and_fwd_flag string,
PULocationID integer,
DOLocationID integer,
payment_type integer,
fare_amount float64,
extra float64,
mta_tax float64,
tip_amount float64,
tolls_amount float64,
improvement_surcharge float64,
total_amount float64,
congestion_surcharge float64
)
OPTIONS (
  format='parquet',
  uris = ['gs://psergios_zoomcamp_2024/green/green_tripdata_2019-*.parquet', 'gs://psergios_zoomcamp_2024/green/green_tripdata_2020-*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE  `de-zoomcamp-2024-411615.trips_data_all.fhv_trip_data`
(
dispatching_base_num STRING, 
pickup_datetime	STRING,
dropOff_datetime datetime,
PUlocationID integer,
DOlocationID integer,
SR_Flag	integer
Affiliated_base_number STRING
)
OPTIONS (
  format='parquet',
  uris = ['gs://psergios_zoomcamp_2024/fhv/fhv_tripdata_2019-*.parquet', 'gs://psergios_zoomcamp_2024/fhv/fhv_tripdata_2020-*.parquet']
);