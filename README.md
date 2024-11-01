# Football Leagues ETL Project with Snowflake and Airflow

This project is a data pipeline built using Apache Airflow and Snowflake to automate the extraction, transformation, and loading (ETL) of football league data. The pipeline is designed to handle data processing, load files into Snowflake stages, and populate tables in Snowflake for further analysis.

## Project Overview
The goal of this project is to streamline the ETL process for managing football league data by leveraging Snowflake's cloud data warehousing capabilities and Airflow’s orchestration tools. The pipeline executes a Python script that processes football league data, generates CSV files, and uploads them to a Snowflake table through a two-step process involving stage loading and table copying.

## Key Components
Airflow DAG:

The DAG (Directed Acyclic Graph) schedules and orchestrates the ETL tasks, including data processing, file upload, and table population in Snowflake.
Each task in the DAG is modular, separating Python-based data processing from SQL-based Snowflake interactions for better maintainability.
Data Processing (Python):

The football_leagues.py script performs data transformations and exports the result as a .csv file.
The script outputs data to a specified path in the Airflow environment, making it accessible for staging in Snowflake.
Snowflake Integration:

SQL scripts (upload_stage.sql and upload_table.sql) are used to interact with Snowflake.
upload_stage.sql uploads the .csv file to a Snowflake stage, while upload_table.sql copies the staged data into the FOOTBALL_LEAGUES table in the PUBLIC schema.
Error Handling and Retry Logic:

The Airflow DAG includes retry logic to handle potential issues in the data pipeline.
The project is designed to handle possible errors during file upload and data loading stages, providing better reliability for production environments.

## Folder Structure
dags/: Contains the main Airflow DAG script and SQL query files.
dags/queries/: SQL scripts for staging and loading data into Snowflake.
dags/data/: Save the .csv file obtained
football_leagues.py: Python script for data processing.
requirements.txt: Lists dependencies for Airflow and Snowflake.
README.md: Project documentation and setup instructions.

## Prerequisites
Apache Airflow (installed locally or through Docker/Astro CLI)
Snowflake account for data storage and querying
Python packages: pandas, snowflake-connector-python, apache-airflow-providers-snowflake (listed in requirements.txt)

## Getting Started
Set up the Airflow environment and configure Snowflake connection parameters.
Adjust the params for the SQL scripts and Python code as needed.
Trigger the DAG to execute the ETL pipeline.

## Future Improvements
Add more extensive logging for easier monitoring and debugging.
Expand the pipeline to include data validation and reporting steps.
Integrate additional data sources or metrics to enhance data analysis.
This project serves as a practical example of how to build an ETL pipeline using Snowflake and Airflow for sports data, demonstrating the potential of cloud data warehousing in data engineering tasks.