Overview
========

Welcome to the assignment using astronmer airflow! This project extracts google analytics json compressed gzip files from AWS S3 bucket and loads into snowflake tables in raw,incremenatlly into staging and finally building aggregated layers for reporting. This project was generated after you ran `astro dev init` using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.
As a pre requisite, astro cli should be install following steps at `https://docs.astronomer.io/software/install-cli`

Project Contents
================

Your Astro project contains the following files and folders:

- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- dags: This folder contains the Python files for our assignment Airflow DAG named `snowflake_load_transform.py`. By default, this directory includes the DAG that runs performs ETL job from amazon S3 bucket to snowflake tables. 
- scripts: This folder under the `/dags` folder is used to place all the SQL scripts for loading raw,stage and aggregated datasets.
- tests: This folder has test_dag.py as very basic unit tests to be run in the container manually using `pytest /tests/dags/test_dag.py`.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It has all the modules to be used by airflow to execute the pipeline.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.
  But for the sake of simplicity we would have to manually add Airflow Connections and Variables in the UI when run for the first time.

Deploy Your Project Locally
===========================

1.Start Airflow on your local machine by running `astro dev start`.

This command will spin up 3 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks

2.Verify that all 3 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either stop your existing Docker containers or change the port.

3.Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

4.Once the airflow UI is up and running manually setup connections for connection Type `Snowflake` and connection Type `s3` as a mandatory step.

5.For this assignment all tables are created in the same schema named `PUBLIC` and database named `ASSIGNMENT` in the snowflake stage named `SNOWFLAKE_STAGE` with AWS bucket named `gaassignment`. Make sure to create the snowflake database,schema,file_format and AWS bucket as a pre requite before starting setup.

6.Setup for AWS S3  connection requires a AWS account with AWS secret key and AWS Access Key.
  The S3 Connection Id created for this project is named as `aws_s3_conn`.
  
7.Setup for snowflake connection requires a snowflake account with warehouse,database and login credentials. AWS secret key and AWS Access Key are also required for integration with S3.
  The snowflake Connection Id created for this project is named as `snowflake_default`.

8.Manually create an airflow variable with key `files_list` and value as `[]`. This would help us hold file names scanned by S3 operator.

9.These manual creation of Variables and connection objects can be automated later by placing them in file `airflow_settings.yaml`.

10.Once all the above is setup properly, the dag named `assignment_snowflake` should be visible without any errors.

About the dag and ETL pipeline
=================================

The dag consists of 9 tasks which are run in chain as the given logic. We have used Dummy operators for `start` and `end` tasks(Dummy email on completion of pipeline).
1.`snowflake_create_raw_tables` = This uses `SnowflakeOperator` to create raw tables in snowflake raw layer as per the DDL scripts in raw folder.


2.`get_bucket_files` = This `PythonOperator` uses S3 hook to read the number of raw files to be loaded into raw zone of Snowflake. 

3.`s3_to_snowflake_raw_zone` = This task uses `S3ToSnowflakeOperator` spawns dynamically into multiple tasks equivalent to the number of files in S3 bucket detected by `get_bucket_files` task.This is used to parallelize the loading of data if we have numerous files in bucket. But can be optimized based on the number of cores and celery executor of airflow.

4.`snowflake_create_stage_tables` = This uses `SnowflakeOperator` to create stage tables as per the DDL scripts in stage folder. We have two tables in the stage layer to be created one being the compact table and other being an exploded table with unnested columns.

5.`snowflake_load_stage_table_compact`= This uses `SnowflakeOperator` to load the compacted table using incremental upserts from the raw table. Load scripts can be found in  stage folder.

6.`snowflake_load_stage_table_exploded`= This uses `SnowflakeOperator` to load the exploded table as full refresh from  the compacted stage table. Load scripts can be found in  stage folder. This table is optional and can be used for analysis if complete unnested columns are to be for data analysis.

7.`snowflake_load_aggregated_views` = This uses `SnowflakeOperator` to create aggregated views from  the compacted stage table. This provides metrics and dimensions  about the data set for reporting. We can even use to create tables in the aggregated layer instead of views.


All variables used in this DAG which can be configurable are:
`BUCKET = 'gaassignment'`
`SCHEMA ='PUBLIC'`
`DATABASE ='ASSIGNMENT'`
`RAW_SCRIPT_PATH = 'scripts/raw'`
`STAGE_SCRIPT_PATH = 'scripts/stage'`
`AGG_SCRIPT_PATH = 'scripts/aggregated'`
`RAW_TABLE_NAME = 'ga_data_raw'`
`STAGE_EXPLODED_TABLE_NAME = 'ga_data_stage_exploded'`
`STAGE_COMPACT_TABLE_NAME = 'ga_data_stage_compact'`
`RAW_FILE_FORMAT='JSON_FORMAT'`
`SNOWFLAKE_STAGE='S3_STAGE'`
`SNOWFLAKE_CONN_ID='snowflake_default'`

All relevant tasks are grouped into TaskGroups for better view in the UI.
Once the DAG has run successfully, we can check the snowflake tables and views in stage and aggregated layers to be loaded.

