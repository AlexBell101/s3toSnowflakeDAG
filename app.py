import streamlit as st
import requests
from datetime import datetime
import base64

# GitHub Configuration
GITHUB_API_URL = "https://api.github.com"
GITHUB_REPO = "AlexBell101/astro-dags"
GITHUB_BRANCH = "main"
GITHUB_TOKEN = st.secrets["GITHUB_TOKEN"]

# Title and Instructions
st.title("Astro DAG Wizard")
st.write(
    "Welcome! Use this wizard to create an Airflow DAG for loading data from S3 to Snowflake. "
    "Before starting, ensure you have created your S3 and Snowflake connections in Astro and have their **connection IDs**."
)

# Step 1: S3 Configuration
st.header("Step 1: S3 Configuration")
s3_conn_id = st.text_input("S3 Connection ID", placeholder="e.g., s3")
s3_stage = st.text_input(
    "Snowflake Stage for S3",
    placeholder="e.g., my_stage",
    help="This stage should point to your S3 bucket and prefix.",
)
s3_destination_key = st.text_input(
    "S3 Destination Key (File Path)",
    placeholder="e.g., Scarf/company-rollups-scarf-export.csv",
    help="Specify the file path in S3 to load into Snowflake.",
)

# Step 2: Snowflake Configuration
st.header("Step 2: Snowflake Configuration")
snowflake_conn_id = st.text_input("Snowflake Connection ID", placeholder="e.g., snowflake")
snowflake_table = st.text_input("Snowflake Table Name", placeholder="e.g., analytics_table")

# Step 3: DAG Configuration
st.header("Step 3: DAG Configuration")
dag_name = st.text_input("DAG Name", placeholder="e.g., s3_to_snowflake_dag")
schedule = st.selectbox(
    "How often should this DAG run?",
    ["Daily", "Hourly", "Weekly", "Custom (Advanced)"],
)
if schedule == "Custom (Advanced)":
    schedule = st.text_input("Custom Schedule Interval", placeholder="e.g., 0 12 * * *")
start_date = st.date_input("Start Date", value=datetime.now())

# Generate and Upload DAG
if st.button("Generate and Push DAG to GitHub"):
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

    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="{snowflake_table}",
        stage="{s3_stage}",
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        pattern=".*\\.csv",
        snowflake_conn_id="{snowflake_conn_id}",
        s3_key="{s3_destination_key}",
        aws_conn_id="{s3_conn_id}",
    )
    """
    # Encode and push to GitHub
    encoded_dag = base64.b64encode(dag_code.encode()).decode()
    url = f"{GITHUB_API_URL}/repos/{GITHUB_REPO}/contents/dags/{dag_name}.py"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    payload = {"message": f"Add {dag_name}.py", "content": encoded_dag, "branch": GITHUB_BRANCH}

    response = requests.put(url, json=payload, headers=headers)
    if response.status_code in [200, 201]:
        st.success(f"{dag_name}.py pushed to GitHub successfully!")
    else:
        st.error(f"Failed to push {dag_name}.py: {response.status_code} - {response.text}")
