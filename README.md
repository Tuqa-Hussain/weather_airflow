# Weather_Data_Pipeline_with_Airflow
![Uploading WhatsApp Image 2023-11-21 at 11.26.47_bdc18ad4.jpg…]()


This Airflow Directed Acyclic Graph (DAG) orchestrates the extraction, transformation, and loading (ETL) process of weather data from the OpenWeatherMap API into a PostgreSQL database.
## Overview

### The pipeline is designed to perform the following tasks:

- Extract: Retrieve weather data for Houston from OpenWeatherMap API.
- Transform: Convert the retrieved data into a structured format, transforming temperatures from Kelvin to Fahrenheit.
- Load: Store the transformed data into PostgreSQL tables and upload joined data to an S3 bucket.

### Prerequisites

Airflow: Ensure you have Apache Airflow installed.
PostgreSQL: Configure a PostgreSQL database and update the connection details in the DAG file.
OpenWeatherMap API Key: Obtain an API key and update it in the DAG file.

### DAG Structure

Dependencies: The DAG has dependencies set to run daily (@daily) starting from November 17, 2023.
Retries: Set to 2 retries with a retry delay of 2 minutes.

### Tasks

- tsk_start_pipeline: Dummy task marking the start of the pipeline.
- tsk_create_table_1: Creates or ensures the existence of the city_look_up table in PostgreSQL.
- tsk_truncate_table: Truncates data from the city_look_up table.
- tsk_uploadS3_to_postgres: Uploads data from an S3 bucket to the city_look_up table.
- tsk_create_table_2: Creates or ensures the existence of the weather_data table in PostgreSQL.
- tsk_is_houston_weather_api_ready: Checks the readiness of the OpenWeatherMap API for Houston weather.
- tsk_extract_houston_weather_data: Extracts Houston weather data from the OpenWeatherMap API.
- transform_load_houston_weather_data: Transforms and loads the extracted weather data into a CSV file (current_weather_data.csv).
- tsk_load_weather_data: Loads the transformed weather data into the weather_data table in PostgreSQL.
- task_join_data: Performs a join operation between weather_data and city_look_up tables in PostgreSQL.
- create a joining data view 

### Usage

Configure Airflow connections for PostgreSQL and the OpenWeatherMap API.
Update the DAG file with the necessary API keys, file paths, and database connections
Place the DAG file in the Airflow DAGs folder and ensure Airflow picks it up.
Monitor the Airflow UI to observe the DAG runs and task statuses.

### Contributors

Abdallah & Tuqa
