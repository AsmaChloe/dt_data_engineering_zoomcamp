-- External Table
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-412318.ny_taxi.green_taxi_2022_parquet_external`
OPTIONS (
  format ="PARQUET",
  uris = ['gs://de-zoomcamp-mage-dtc-de-course-412318/green_taxi_data_2022.parquet']
);

-- Internal Table
CREATE OR REPLACE TABLE `dtc-de-course-412318.ny_taxi.green_taxi_2022_parquet` AS
SELECT 
  VendorID,
  CAST(TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS date) AS lpep_pickup_date,
  CAST(TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS date) AS lpep_dropoff_date,
  passenger_count,
  trip_distance,
  RatecodeID,
  store_and_fwd_flag,
  PULocationID,
  DOLocationID,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge
FROM `dtc-de-course-412318.ny_taxi.green_taxi_2022_parquet_external`;

-- Question 1: 
-- What is count of records for the 2022 Green Taxi Data??
select count(*) from dtc-de-course-412318.ny_taxi.green_taxi_2022_parquet;
-- 840402

-- Question 2:
-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
select count(distinct PULocationID) from  `dtc-de-course-412318.ny_taxi.green_taxi_2022_parquet_external` ;
-- 0 MB estimated
select count(distinct PULocationID) from  `dtc-de-course-412318.ny_taxi.green_taxi_2022_parquet` ;
-- 6.41MB MB estimated

-- Question 3:
-- How many records have a fare_amount of 0?
select count(*) from `dtc-de-course-412318.ny_taxi.green_taxi_2022_parquet` where fare_amount = 0;
-- 1622

-- Question 4:
-- What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this --strategy)
-- Partition by lpep_pickup_datetime Cluster on PUlocationID
CREATE OR REPLACE TABLE `dtc-de-course-412318.ny_taxi.green_taxi_2022_parquet_partitionned_pudatetime_clustered_puid`
PARTITION BY lpep_pickup_date
CLUSTER BY PUlocationID AS
SELECT * FROM `dtc-de-course-412318.ny_taxi.green_taxi_2022_parquet`
;

-- Question 5:
-- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
select
  distinct PULocationID
from `dtc-de-course-412318.ny_taxi.green_taxi_2022_parquet`
WHERE lpep_pickup_date > '2022-06-01' AND lpep_pickup_date <= '2022-06-30';
-- 12.82 MB

select
  distinct PULocationID
from `dtc-de-course-412318.ny_taxi.green_taxi_2022_parquet_partitionned_pudatetime_clustered_puid`
WHERE lpep_pickup_date > '2022-06-01' AND lpep_pickup_date <= '2022-06-30';
-- 1.12 MB

-- Question 6:
-- Where is the data stored in the External Table you created?
-- GCP Bucket

-- Question 7:
-- It is best practice in Big Query to always cluster your data:
-- False