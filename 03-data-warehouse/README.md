## What is a data warehouse?
- OLAP (Online Analytical Processing) solution.

- Primarily used for data analysis, reporting, and business intelligence.
- Optimized for reading and querying large volumes of historical data to support decision-making processes.
- May contain data marts for  specific business line.

## BigQuery
- Fully managed serverless data warehouse, as well, analytics platform offered by Google Cloud.
- BigQuery maximizes flexibility by separating the compute engine that analyzes your data from your storage.

#### Cache query results
BigQuery caches query results to improve query performance and reduce costs. This can lead to unexpected query results if you're not aware of it. If you want to ensure that each query reflects the most up-to-date data, you can disable caching temporarily or set cache expiration policies.

#### Public datasets
You can find available public datasets in BigQuery by searching for them using keywords or dataset names in the BigQuery Console or UI.

#### Pricing Models
*On Demand*: $5 per TB of data processed.  
*Flat Rate*: based on the number or pre requested slots. 100 slots costs ~ $2,000/mos which is ~400TB of data processed on demand.

##  BigQuery Partitoning and Clustering
- A partitioned table is a type of database table in which the data is divided into multiple smaller sub-tables or partitions based on a specific column or set of columns, typically a date or timestamp column.
- The primary purpose of partitioning tables is to improve query performance and data management by allowing the database system to read and write only the relevant partitions when executing queries or performing maintenance operations. When done correctly, partitioning can considerably improve performance.
- Within a partition you can further cluster the data by another attribute. Data will then be grouped within the partition by the clustering attribute. This can also increase querying efficiency.
- Number of partitions limit is 4000.
- Table with data size < 1 GB, don’t show significant improvement with partitioning and clustering.
- Can be specified up to four clustering columns.

## ML in BigQuery
- It is possible to build, run, and deploy a model in SQL, no need for other languages like python or R.
- The model can be built directly in the data warehouse. There is no need to export data into a different system.
- BigQuery can automatically handle many aspects of the ML process: Feature engineering, Hyperparameter tuning, and Data splitting.
- Provides a selection of basic ML models and the ability to create custom models in TensorFlow. It also provides various error metrics for model evaluation.
- The ML Model can be deployed using docker.
- [Steps to extract and deploy model.](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/03-data-warehouse/extract_model.md)
        

## Homework
- Use the 2022 Green Taxi Trip Record Parquet Files from the New York City Taxi Data found [here](
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and load to bucket in GCS.
- File used: **homework_big_query.sql**.
- Main Steps:
  - Create external and materealized table (using data uploaded to GCS Buckets);
  - Perform queries and checks;
  - Partition and cluster the table.
