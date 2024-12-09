import streamlit as st
from datetime import datetime

# Title and Introduction
st.title("Astro DAG Wizard")
st.write(
    "Welcome! This wizard will help you generate an Airflow DAG for transferring files from S3 to Snowflake. "
    "No prior Airflow experience needed!"
)

# Connection Reminder
st.warning(
    "Before starting, make sure you have added your S3 and Snowflake connections in Astro. "
    "You’ll need the **connection IDs** to proceed. You can add connections via the 'Connections' tab in the Astro UI."
)

# Step 1: S3 Configuration
st.header("Step 1: S3 Configuration")
s3_conn_id = st.text_input("S3 Connection ID", placeholder="e.g., s3")
s3_bucket = st.text_input("S3 Bucket Name", placeholder="e.g., my-data-bucket")
s3_key = st.text_input("S3 Destination Key (File Path)", placeholder="e.g., folder/subfolder/file.csv")

# Step 2: Snowflake Configuration
st.header("Step 2: Snowflake Configuration")
snowflake_conn_id = st.text_input("Snowflake Connection ID", placeholder="e.g., snowflake")
snowflake_table = st.text_input("Snowflake Table Name", placeholder="e.g., analytics_table")
create_table = st.checkbox("Create Snowflake Table if not exists", value=True)

if create_table:
    table_columns = st.text_area(
        "Table Columns (Name and Type)",
        placeholder="e.g., id INT, name STRING, created_at TIMESTAMP",
    )

# Step 3: DAG Configuration
st.header("Step 3: DAG Configuration")
dag_name = st.text_input("DAG Name", placeholder="e.g., s3_to_snowflake_dag")
schedule = st.selectbox(
    "How often should this DAG run?",
    ["Daily", "Hourly", "Weekly", "Custom (Advanced)"]
)
if schedule == "Custom (Advanced)":
    schedule = st.text_input("Custom Schedule Interval", placeholder="e.g., 0 12 * * *")
start_date = st.date_input("Start Date", value=datetime.now())

# Generate the DAG code
if st.button("Generate DAG Code"):
    dag_code = f"""
from airflow import DAG
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
{'from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator' if create_table else ''}
from datetime import datetime

default_args = {{
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}}

with DAG(
    dag_id="{dag_name}",
    default_args=default_args,
    schedule_interval="{schedule}",
    start_date=datetime({start_date.year}, {start_date.month}, {start_date.day}),
    catchup=False,
) as dag:

    {'create_table_task = SnowflakeOperator(task_id="create_table", sql="CREATE TABLE IF NOT EXISTS {snowflake_table} ({table_columns})", snowflake_conn_id="{snowflake_conn_id}")' if create_table else ''}

    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="{snowflake_table}",
        stage="{s3_conn_id}_stage",
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        pattern=".*\\.csv",
        snowflake_conn_id="{snowflake_conn_id}",
        s3_bucket_name="{s3_bucket}",
        s3_key="{s3_key}",
    )

    {'create_table_task >> load_to_snowflake' if create_table else ''}
    """
    st.code(dag_code, language="python")
    st.success("DAG code generated successfully!")
