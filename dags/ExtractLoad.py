from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import io
import logging

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(1996, 8, 21),
    'end_date': datetime(2012, 1, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'postgres_to_gcs_to_bigquery', 
    default_args=default_args,
    description='Daily ingestion from PostgreSQL to GCS to BigQuery (1996-2012)',
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=3,
    tags=['etl', 'postgres', 'gcs', 'bigquery', 'historical'],
)

# Configuration
GCP_PROJECT_ID = 'noted-cider-471809-d8'
GCS_BUCKET_NAME = 'beer-reviews'
BIGQUERY_DATASET = 'beer_reviews_staged'
POSTGRES_TABLE = 'reviews.beer_reviews'
POSTGRES_CONN_ID = 'postgres_default'
GCP_CONN_ID = 'google_cloud_default'

def extract_postgres_to_gcs(**context):
    """
    Extract data from PostgreSQL for a specific date and upload to GCS.
    Always create a file, even if there are zero data points.
    """
    execution_date = context['ds']
    logging.info(f"Processing data for date: {execution_date}")

    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    sql_query = f"""
        SELECT 
            reviewid as review_id,
            beerbeerid as beer_id,
            beername as beer_name,
            beerbrewerid as brewery_id,
            beerabv as beer_abv,
            beerstyle as beer_style,
            reviewoverall as review_overall,
            reviewaroma as review_aroma,
            reviewappearance as review_appearance,
            reviewpalate as review_palate,
            reviewtaste as review_taste,
            reviewtime as review_time,
            reviewprofilename as reviewer_name,
            reviewtext as review_text
        FROM {POSTGRES_TABLE}
        WHERE DATE(to_timestamp(reviewtime)) = '{execution_date}'
        ORDER BY reviewtime;
    """

    df = postgres_hook.get_pandas_df(sql_query)

    logging.info(f"Extracted {len(df)} rows for date: {execution_date}")

    # Always create a CSV file, even if empty
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    year = execution_date[:4]
    month = execution_date[5:7]
    day = execution_date[8:10]

    object_name = f"raw/year={year}/month={month}/day={day}/beer_reviews_{execution_date}.csv"

    gcs_hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=object_name,
        data=csv_data,
        mime_type='text/csv'
    )

    logging.info(f"Successfully uploaded data to gs://{GCS_BUCKET_NAME}/{object_name}")
    return f"gs://{GCS_BUCKET_NAME}/{object_name}"


def validate_gcs_file(**context):
    execution_date = context['ds']
    year = execution_date[:4]
    month = execution_date[5:7]
    day = execution_date[8:10]

    object_name = f"raw/year={year}/month={month}/day={day}/beer_reviews_{execution_date}.csv"

    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    if gcs_hook.exists(bucket_name=GCS_BUCKET_NAME, object_name=object_name):
        file_size = gcs_hook.get_size(bucket_name=GCS_BUCKET_NAME, object_name=object_name)
        logging.info(f"File validation successful. Size: {file_size} bytes")
        return True
    else:
        logging.warning(f"File not found in GCS: gs://{GCS_BUCKET_NAME}/{object_name}")
        return False


#############################
#          Tasks            #
#############################

create_gcs_bucket = GCSCreateBucketOperator(
    task_id='create_gcs_bucket',
    bucket_name=GCS_BUCKET_NAME,
    project_id=GCP_PROJECT_ID,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)
create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_bigquery_dataset',
    dataset_id=BIGQUERY_DATASET,
    project_id=GCP_PROJECT_ID,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)
extract_to_gcs = PythonOperator(
    task_id='extract_postgres_to_gcs',
    python_callable=extract_postgres_to_gcs,
    dag=dag,
)
validate_upload = PythonOperator(
    task_id='validate_gcs_upload',
    python_callable=validate_gcs_file,
    dag=dag,
)
load_to_bigquery = GCSToBigQueryOperator(
    task_id='load_gcs_to_bigquery',
    bucket=GCS_BUCKET_NAME,
    source_objects=[
        "raw/year={{ ds[:4] }}/month={{ ds[5:7] }}/day={{ ds[8:10] }}/beer_reviews_{{ ds }}.csv"
    ],
    destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.daily_beer_reviews",
    schema_fields=[
        {'name': 'review_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'beer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'beer_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'brewery_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'beer_abv', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'beer_style', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'review_overall', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'review_aroma', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'review_appearance', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'review_palate', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'review_taste', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'review_time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'reviewer_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'review_text', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,
    allow_quoted_newlines=True,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)


data_quality_check = BigQueryInsertJobOperator(
    task_id="bigquery_data_quality_check",
    configuration={
        "query": {
            "query": f"""
                SELECT 
                    '{{{{ ds }}}}' as processing_date,
                    COUNT(*) as row_count,
                    COUNT(DISTINCT review_id) as unique_reviews,
                    MIN(review_time) as min_review_time,
                    MAX(review_time) as max_review_time
                FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.daily_beer_reviews`
                WHERE DATE(review_time) = '{{{{ ds }}}}'
            """,
            "useLegacySql": False,
        }
    },
    location="US",  
    project_id=GCP_PROJECT_ID,
    dag=dag,
)


create_gcs_bucket >> extract_to_gcs
create_bq_dataset >> load_to_bigquery
extract_to_gcs >> validate_upload >> load_to_bigquery >> data_quality_check
