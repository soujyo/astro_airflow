"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
import ast
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.utils.task_group import TaskGroup

BUCKET = 'gaassignment'
SCHEMA ='PUBLIC'
DATABASE ='ASSIGNMENT'
RAW_SCRIPT_PATH = 'scripts/raw'
STAGE_SCRIPT_PATH = 'scripts/stage'
AGG_SCRIPT_PATH = 'scripts/aggregated'
RAW_TABLE_NAME = 'ga_data_raw'
STAGE_EXPLODED_TABLE_NAME = 'ga_data_stage_exploded'
STAGE_COMPACT_TABLE_NAME = 'ga_data_stage_compact'
RAW_FILE_FORMAT='JSON_FORMAT'
SNOWFLAKE_STAGE='S3_STAGE'
SNOWFLAKE_CONN_ID='snowflake_default'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


with DAG("assignment_snowflake", max_active_runs=1, default_args=default_args,
         schedule_interval=None, render_template_as_native_obj=True, catchup=False
         ) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="creating_tables_snowflake"):
        snowflake_create_raw_tables = SnowflakeOperator(
            task_id='snowflake_create_raw_tables',
            sql=f"{RAW_SCRIPT_PATH}/create_table.sql",
            snowflake_conn_id="snowflake_default",
            schema=SCHEMA,
            params={
                "raw_schema": SCHEMA,
                "raw_db": DATABASE,
                "raw_table": RAW_TABLE_NAME
            },
        )

        snowflake_create_stage_tables = SnowflakeOperator(
            task_id='snowflake_create_stage_tables',
            sql=f"{STAGE_SCRIPT_PATH}/create_table.sql",
            snowflake_conn_id="snowflake_default",
            schema=SCHEMA,
            params={
                "stage_schema": SCHEMA,
                "stage_db": DATABASE,
                "stage_table_exploded": STAGE_EXPLODED_TABLE_NAME,
                "stage_table_compact": STAGE_COMPACT_TABLE_NAME
            },
        )

    with TaskGroup(group_id="loading_stage_snowflake", dag=dag):
        snowflake_load_stage_table_compact = SnowflakeOperator(
            task_id='snowflake_load_stage_tables_compact',
            sql=f"{STAGE_SCRIPT_PATH}/load_table_compact.sql",
            snowflake_conn_id="snowflake_default",
            schema='PUBLIC',
            params={
                "raw_schema": SCHEMA,
                "raw_db": DATABASE,
                "raw_table": RAW_TABLE_NAME,
                "stage_schema": SCHEMA,
                "stage_db": DATABASE,
                "stage_table": STAGE_COMPACT_TABLE_NAME
            },
        )
        snowflake_load_stage_table_exploded = SnowflakeOperator(
            task_id='snowflake_load_stage_tables_exploded',
            sql=f"{STAGE_SCRIPT_PATH}/load_table_exploded.sql",
            snowflake_conn_id="snowflake_default",
            schema='PUBLIC',
            params={
                "raw_schema": SCHEMA,
                "raw_db": DATABASE,
                "stage_schema": SCHEMA,
                "stage_db": DATABASE,
                "stage_table_exploded": STAGE_EXPLODED_TABLE_NAME,
                "stage_table_compact": STAGE_COMPACT_TABLE_NAME
            },
        )

    with TaskGroup(group_id="loading_aggregates_snowflake", dag=dag):
        snowflake_load_aggregated_views = SnowflakeOperator(
            task_id='loading_aggregated_views_snowflake',
            sql=f"{AGG_SCRIPT_PATH}/aggregated_views.sql",
            snowflake_conn_id="snowflake_default",
            schema='PUBLIC',
            params={
                "agg_schema": SCHEMA,
                "agg_db": DATABASE,
                "stage_schema": SCHEMA,
                "stage_db": DATABASE,
                "stage_table_compact": STAGE_COMPACT_TABLE_NAME,
                "stage_table_exploded": STAGE_EXPLODED_TABLE_NAME
            },
        )

    def read_from_bucket(ti):
        s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
        files = s3_hook.list_keys(bucket_name=BUCKET)
        print("files: {}".format(files))
        ti.xcom_push(key='all_files', value=files)
        Variable.set("files_list", list(files))


    with TaskGroup(group_id="loading_from_s3_to_snowflake", dag=dag):
        get_bucket_files = PythonOperator(
            task_id='get_bucket_files',
            python_callable=read_from_bucket,
        )
        s3_to_snowflake_raw_zone = [S3ToSnowflakeOperator(
            task_id=f'upload_to_snowflake_raw_from_s3_{index}',
            s3_keys=[file],
            stage=SNOWFLAKE_STAGE,
            table=RAW_TABLE_NAME,
            schema=SCHEMA,
            file_format=RAW_FILE_FORMAT,
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
        ) for index, file in enumerate(ast.literal_eval(Variable.get("files_list")))]

    email_operator = EmptyOperator(task_id="Dummy_Email_on_completion", dag=dag)


    chain(
        start,
        snowflake_create_raw_tables,
        get_bucket_files,
        s3_to_snowflake_raw_zone,
        snowflake_create_stage_tables,
        snowflake_load_stage_table_compact,
        snowflake_load_stage_table_exploded,
        snowflake_load_aggregated_views,
        email_operator
    )
