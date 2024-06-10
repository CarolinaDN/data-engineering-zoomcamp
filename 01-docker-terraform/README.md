# Introduction

Docker allows to put everything an application needs inside a container - sort of a box that contains everything: OS, system-level libraries, python, etc.

## When to containerize and use docker?
* To set up things locally for experiments
* To do integration tests, CI/CD
* For batch jobs (AWS Batch, Kubernetes jobs, etc — outside of the scope)
* To program with Spark
* Serverless (AWS Lambda)


## Postgres
* Used for practicing with SQL
* It’s a general purpose database
* It’s powerful enough and sometimes also used as a data warehouse - a special database for analytical queries


## Terraform
* What is Terraform?
  * Open-source tool by HashiCorp, used for provisioning infrastructure resources
  * Supports DevOps best practices for change management
  * Managing configuration files in source control to maintain an ideal provisioning state for testing and production environments
* Why use it?
  * Easier collaboration: push to repo
  * Reproducibility
  * Ensure resources are removed: avoid being charge of what is not neccessary
* What is IaC (=Infrastructure-as-Code)?
  * Build, change, and manage your infrastructure in a safe, consistent, and repeatable way by defining resource configurations that you can version, reuse, and share.
* Some advantages of IaC: 
   * Infrastructure lifecycle management
   * Version control commits
   * Very useful for stack-based deployments, and with cloud providers such as AWS, GCP, Azure, K8S…
   * State-based approach to track resource changes throughout deployments

## Homework
This repository contains most of the tests and learnings that I used for the course.  
The homework of this module was done in the end of the videos and followed the steps:
1) Be in the terminal in the following path: `.\data-engineering-zoomcamp\01-docker-terraform\2_docker_sql\starting_docker`. 

2) Use the command to start the docker `docker-compose up -d`. Which is using:  
   a. `Dockerfile` that contains the libraries to download, python version and python script.  

   b. The already set up yaml `docker-compose.yaml`, that allows to have all necessary variables. 

   c. Python script `ingest_data.py`, with the ingestion pipeline to insert dataset `ny_taxi_postgres_data` into postgresql.  
   
3) Connect to pgadmin using localhost:8080.

4) Download the data and put it into Postgres (with jupyter notebooks or with a pipeline):  
   a. [green_tripdata](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz)

   b. [taxi_zone](https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv)



4)  Perform the requested queries, that can be found in 1st_module.sql.
