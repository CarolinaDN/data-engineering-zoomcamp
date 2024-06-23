-- Create an external table using the Green Taxi Trip Records Data for 2022
CREATE OR REPLACE EXTERNAL TABLE `vivid-grammar-424416-a0.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://mage-zoomcamp-carolina/green_taxi_2022/green_tripdata_2022-*.parquet']
);


-- Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table)
CREATE OR REPLACE TABLE vivid-grammar-424416-a0.ny_taxi.green_tripdata AS
SELECT * FROM vivid-grammar-424416-a0.ny_taxi.external_green_tripdata;


-- Question 1: What is count of records for the 2022 Green Taxi Data?
SELECT COUNT(*) FROM vivid-grammar-424416-a0.ny_taxi.green_tripdata;
-- Answer: 840,402


-- Question 2: 
-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Materialized  Table?
SELECT COUNT(DISTINCT PULocationID) FROM vivid-grammar-424416-a0.ny_taxi.green_tripdata;
SELECT COUNT(DISTINCT PULocationID) FROM vivid-grammar-424416-a0.ny_taxi.external_green_tripdata;
-- Answer: 0 MB for the External Table and 6.41MB for the Materialized Table


-- Question 3: How many records have a fare_amount of 0?
SELECT COUNT(*) FROM vivid-grammar-424416-a0.ny_taxi.green_tripdata WHERE fare_amount=0;
-- Answer: 1,622


-- Question 4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
CREATE OR REPLACE TABLE vivid-grammar-424416-a0.ny_taxi.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM vivid-grammar-424416-a0.ny_taxi.green_tripdata;
-- Answer: Partition by lpep_pickup_datetime and Cluster by PUlocationID


-- Question 5:
-- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
-- Check non partitioned and table from question 4
SELECT DISTINCT(PULocationID)
FROM vivid-grammar-424416-a0.ny_taxi.green_tripdata
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT(PULocationID)
FROM vivid-grammar-424416-a0.ny_taxi.green_tripdata_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
-- Answer: 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table


-- Question 6: Where is the data stored in the External Table you created?
-- Answer: GCP Bucket


-- Question 7: It is best practice in Big Query to always cluster your data.
-- Answer: False


-- Question 8: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
SELECT COUNT(*) FROM vivid-grammar-424416-a0.ny_taxi.green_tripdata;
-- Answer: 0 bytes. The query has been already run in question 1, thus, the result is cached.
