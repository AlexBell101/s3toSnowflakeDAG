from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Replace with your DAG name
with DAG(
    dag_id='s3_to_snowflake_dag',
    default_args=default_args,
    description='A DAG to load data from S3 to Snowflake',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Upload local file to S3 (optional)
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_file_to_s3',
        filename='/path/to/your/local/file.csv',  # Replace with your local file path
        dest_key='your-s3-folder/file.csv',  # Replace with your S3 key
        dest_bucket_name='your-s3-bucket-name',  # Replace with your S3 bucket name
        aws_conn_id='aws_default',  # Replace with your AWS connection ID
    )

    # Task 2: Transfer from S3 to Snowflake
    load_to_snowflake = SnowflakeOperator(
        task_id='load_s3_to_snowflake',
        sql="""
        COPY INTO your_table_name
        FROM 's3://your-s3-bucket-name/your-s3-folder/'
        STORAGE_INTEGRATION = 'your_storage_integration'
        FILE_FORMAT = (type = 'CSV' field_optionally_enclosed_by = '"');
        """,
        snowflake_conn_id='snowflake_default',  # Replace with your Snowflake connection ID
    )

    # Define task dependencies
    upload_to_s3 >> load_to_snowflake
