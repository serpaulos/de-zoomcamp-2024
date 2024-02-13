--Create an external table using the Green Taxi Trip Records Data for 2022.
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-2024-411615.de_zoomcamp_2024_ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://zoomcamp_2004_bucket/green_tripdata_2022-*.parquet']
);

--Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).
CREATE OR REPLACE TABLE de-zoomcamp-2024-411615.de_zoomcamp_2024_ny_taxi.green_tripdata_non_partitoned AS
SELECT * FROM de-zoomcamp-2024-411615.de_zoomcamp_2024_ny_taxi.external_green_tripdata;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE de-zoomcamp-2024-411615.de_zoomcamp_2024_ny_taxi.green_tripdata_non_partitoned AS
SELECT * FROM de-zoomcamp-2024-411615.de_zoomcamp_2024_ny_taxi.external_green_tripdata;

--Question 1: What is count of records for the 2022 Green Taxi Data??
SELECT count(*) FROM `de-zoomcamp-2024-411615.de_zoomcamp_2024_ny_taxi.external_green_tripdata`;

--Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
--What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

SELECT count(distinct PULocationID) FROM `de-zoomcamp-2024-411615.de_zoomcamp_2024_ny_taxi.external_green_tripdata`;
SELECT count(distinct PULocationID) FROM `de-zoomcamp-2024-411615.de_zoomcamp_2024_ny_taxi.green_tripdata_non_partitoned`;

--Question 3: How many records have a fare_amount of 0?
SELECT count(fare_amount) FROM `de-zoomcamp-2024-411615.de_zoomcamp_2024_ny_taxi.external_green_tripdata` where fare_amount = 0;


-- Question 4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? 
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE de-zoomcamp-2024-411615.de_zoomcamp_2024_ny_taxi.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM de-zoomcamp-2024-411615.de_zoomcamp_2024_ny_taxi.external_green_tripdata;

--Question 5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
--06/01/2022 and 06/30/2022 (inclusive)
SELECT distinct PULocationID FROM `de-zoomcamp-2024-411615.de_zoomcamp_2024_ny_taxi.green_tripdata_non_partitoned`
where lpep_pickup_datetime between timestamp('2022-01-06') and timestamp('2022-01-30');

SELECT distinct PULocationID FROM `de-zoomcamp-2024-411615.de_zoomcamp_2024_ny_taxi.green_tripdata_partitoned_clustered`
where lpep_pickup_datetime between timestamp('2022-01-06') and timestamp('2022-01-30');

