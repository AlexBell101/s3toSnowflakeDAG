import streamlit as st
import requests
from datetime import datetime
import base64

# GitHub Configuration
GITHUB_API_URL = "https://api.github.com"
GITHUB_REPO = "AlexBell101/astro-dags"  # Replace with your GitHub repository name
GITHUB_BRANCH = "main"  # Replace with your target branch
GITHUB_TOKEN = st.secrets["GITHUB_TOKEN"]  # Access GitHub token from secrets

# Title
st.title("Astro Project Wizard")
st.write("Welcome! This wizard will help you create a working Airflow DAG for uploading data from S3 to Snowflake. "
         "Ensure you have preconfigured your connections in Astro and have the connection IDs handy.")

# Step 1: DAG Configuration
st.header("Step 1: DAG Configuration")
dag_name = st.text_input("DAG Name", placeholder="e.g., s3_to_snowflake_dag")
schedule = st.selectbox(
    "How often should this DAG run?",
    ["Daily", "Hourly", "Weekly", "Custom (Advanced)"]
)
if schedule == "Custom (Advanced)":
    schedule = st.text_input("Custom Schedule Interval", placeholder="e.g., 0 12 * * *")
start_date = st.date_input("Start Date", value=datetime.now())

# Step 2: S3 Configuration
st.header("Step 2: S3 Configuration")
s3_key = st.text_input("S3 File Path (Key)", placeholder="e.g., my-folder/my-file.csv")
s3_stage_name = st.text_input("Snowflake Stage Name", placeholder="e.g., your_stage_name")
s3_connection_id = st.text_input("S3 Connection ID", placeholder="e.g., s3")

# Step 3: Snowflake Configuration
st.header("Step 3: Snowflake Configuration")
snowflake_connection_id = st.text_input("Snowflake Connection ID", placeholder="e.g., snowflake")
snowflake_table = st.text_input("Snowflake Table Name", placeholder="e.g., your_table_name")
snowflake_database = st.text_input("Snowflake Database Name", placeholder="e.g., your_database_name")
snowflake_schema = st.text_input("Snowflake Schema Name", placeholder="e.g., your_schema_name")

# Generate Files
if st.button("Generate and Push Astro Project to GitHub"):
    # Create DAG File
    dag_code = f"""
from airflow import DAG
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
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

    # Task: Load Data into Snowflake
    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="{snowflake_table}",
        database="{snowflake_database}",
        schema="{snowflake_schema}",
        stage="{s3_stage_name}",
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        pattern=".*\\.csv",
        snowflake_conn_id="{snowflake_connection_id}",
        aws_conn_id="{s3_connection_id}",
    )
    """

    # Push to GitHub
    encoded_dag = base64.b64encode(dag_code.encode()).decode()
    url = f"{GITHUB_API_URL}/repos/{GITHUB_REPO}/contents/dags/{dag_name}.py"
    payload = {"message": f"Add {dag_name}.py", "content": encoded_dag, "branch": GITHUB_BRANCH}
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    response = requests.put(url, json=payload, headers=headers)
    if response.status_code in [200, 201]:
        st.success(f"{dag_name}.py pushed to GitHub!")
    else:
        st.error(f"Failed to push {dag_name}.py: {response.status_code} - {response.text}")
