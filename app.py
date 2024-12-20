import streamlit as st
import requests
from datetime import datetime
import base64

# GitHub Configuration
GITHUB_API_URL = "https://api.github.com"
GITHUB_REPO = "AlexBell101/astro-dags"  # Replace with your GitHub repository name
GITHUB_BRANCH = "main"  # Replace with your target branch
GITHUB_TOKEN = st.secrets["GITHUB_TOKEN"]  # Access GitHub token from secrets

# Custom CSS
st.markdown("""
    <style>
        body, input, select, textarea { font-family: 'Inter', sans-serif; }
        h1 { color: #1A73E8; }
        .stButton>button { background-color: #1A73E8; color: white; }
        .stButton>button:hover { background-color: #135BA1; }
    </style>
""", unsafe_allow_html=True)

# Intro
st.title("Astro DAG Wizard")
st.write("Welcome! This wizard generates an Airflow DAG for loading data from S3 to Snowflake.")
st.write("Ensure your **S3 and Snowflake connections** are preconfigured in Astro and have the connection IDs handy.")

# S3 Configuration
st.header("Step 1: S3 Configuration")
s3_conn_id = st.text_input("S3 Connection ID", placeholder="e.g., s3")
s3_key = st.text_input("S3 Key (File Path)", placeholder="e.g., Scarf/your-file.csv")

# Snowflake Configuration
st.header("Step 2: Snowflake Configuration")
snowflake_conn_id = st.text_input("Snowflake Connection ID", placeholder="e.g., snowflake")
snowflake_database = st.text_input("Snowflake Database Name", placeholder="e.g., TEST_DB")
snowflake_schema = st.text_input("Snowflake Schema Name", placeholder="e.g., public")
snowflake_table = st.text_input("Snowflake Table Name", placeholder="e.g., scarf")
snowflake_stage = st.text_input("Snowflake Stage Name", placeholder="e.g., s3_stage")

# DAG Configuration
st.header("Step 3: DAG Configuration")
dag_name = st.text_input("DAG Name", placeholder="e.g., s3_to_snowflake_dag")
schedule = st.selectbox(
    "Schedule Interval",
    ["Every hour", "Every day", "Every week", "Custom (Advanced)"]
)
if schedule == "Custom (Advanced)":
    schedule = st.text_input("Enter Cron Expression", placeholder="e.g., 0 * * * *")
else:
    schedule = {"Every hour": "@hourly", "Every day": "@daily", "Every week": "@weekly"}[schedule]
start_date = st.date_input("Start Date", value=datetime.now())

# Generate and Push DAG
if st.button("Generate and Push DAG"):
    # DAG Code
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
        database="{snowflake_database}",
        schema="{snowflake_schema}",
        snowflake_conn_id="{snowflake_conn_id}",
        stage="{snowflake_stage}",
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        pattern=".*\\.csv",
        s3_key="{s3_key}",
        aws_conn_id="{s3_conn_id}",
    )
    """

    # Encode and push to GitHub
    encoded_dag = base64.b64encode(dag_code.encode()).decode()
    dag_path = f"dags/{dag_name}.py"
    url = f"{GITHUB_API_URL}/repos/{GITHUB_REPO}/contents/{dag_path}"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}

    # Check if the file already exists
    response = requests.get(url, headers=headers)
    if response.status_code == 200:  # File exists, get SHA for update
        sha = response.json().get("sha")
    else:  # File doesn't exist
        sha = None

    # Prepare the payload
    payload = {
        "message": f"Add {dag_name}.py",
        "content": encoded_dag,
        "branch": GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    # Push the file to GitHub
    upload_response = requests.put(url, json=payload, headers=headers)

    if upload_response.status_code in [200, 201]:
        st.success(f"DAG {dag_name}.py successfully pushed to GitHub!")
    else:
        st.error(f"Failed to push DAG: {upload_response.status_code} - {upload_response.text}")
