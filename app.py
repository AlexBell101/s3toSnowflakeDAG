import streamlit as st
import requests
from datetime import datetime
import base64

# GitHub Configuration
GITHUB_API_URL = "https://api.github.com"
GITHUB_REPO = "AlexBell101/astro-dags"  # Replace with your GitHub repository name
GITHUB_BRANCH = "main"  # Replace with your target branch
GITHUB_TOKEN = st.secrets["GITHUB_TOKEN"]  # Access GitHub token from secrets

# Astro API Configuration
ASTRO_API_URL = st.secrets["ASTRO_API"]  # Access Astro API URL from secrets

# Custom CSS
st.markdown("""
    <style>
        html, body, [class*="css"] {
            font-family: 'Inter', sans-serif;
            color: #32325D;
        }
        h1 { font-weight: 600; color: #0070F3 !important; }
        h2, h3 { font-weight: 500; color: #0070F3 !important; }
        .stTextInput > label, .stNumberInput > label, .stDateInput > label { color: #A276FF; }
        div.stButton > button { background-color: #2B6CB0; color: #FFFFFF; padding: 8px 16px; font-weight: 500; }
        div.stButton > button:hover { background-color: #3182CE; }
    </style>
""", unsafe_allow_html=True)

# Title
st.title("Astro Project Wizard")
st.write("Welcome! This wizard helps you create an Astro project to transfer files from S3 to Snowflake, even if you're new to Airflow.")

# Step 1: S3 Configuration
st.header("Step 1: S3 Configuration")
s3_bucket_name = st.text_input("S3 Bucket Name", placeholder="e.g., scarfdata")
s3_key = st.text_input("S3 File Key (Full Path)", placeholder="e.g., Scarf/company-rollups.csv")
s3_connection_id = st.text_input("S3 Connection ID", placeholder="e.g., s3")

# Step 2: Snowflake Configuration
st.header("Step 2: Snowflake Configuration")
snowflake_table = st.text_input("Snowflake Table Name", placeholder="e.g., company_rollups")
snowflake_stage = st.text_input("Snowflake Stage Name", placeholder="e.g., your_stage_name")
snowflake_connection_id = st.text_input("Snowflake Connection ID", placeholder="e.g., snowflake")

# Optional: Create Snowflake Table
create_table = st.checkbox("Create Snowflake Table (if it doesn't exist)")
if create_table:
    table_columns = st.text_area(
        "Table Columns (SQL Format)",
        placeholder="e.g., id INT, name STRING, created_at TIMESTAMP"
    )

# Step 3: DAG Configuration
st.header("Step 3: DAG Configuration")
dag_name = st.text_input("DAG Name", placeholder="e.g., s3_to_snowflake_dag")
schedule = st.selectbox(
    "How often should this DAG run?",
    ["Once", "Daily", "Hourly", "Weekly", "Custom"]
)
if schedule == "Custom":
    schedule = st.text_input("Custom Schedule Interval", placeholder="e.g., 0 12 * * *")
else:
    schedule = {"Once": None, "Daily": "@daily", "Hourly": "@hourly", "Weekly": "@weekly"}[schedule]
start_date = st.date_input("Start Date", value=datetime.now())

# Generate DAG Code
if st.button("Generate and Push Astro Project to GitHub"):
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
    schedule_interval={repr(schedule)},
    start_date=datetime({start_date.year}, {start_date.month}, {start_date.day}),
    catchup=False,
) as dag:

    {"# Task: Create Snowflake Table\n    create_table_task = SnowflakeOperator(\n        task_id='create_table',\n        sql='CREATE TABLE IF NOT EXISTS {snowflake_table} ({table_columns})',\n        snowflake_conn_id='{snowflake_connection_id}',\n    )" if create_table else ""}

    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="{snowflake_table}",
        stage="{snowflake_stage}",
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        pattern=".*\\.csv",
        snowflake_conn_id="{snowflake_connection_id}",
        s3_key="{s3_key}",
        s3_bucket_name="{s3_bucket_name}",
        aws_conn_id="{s3_connection_id}",
    )

    {"create_table_task >> load_to_snowflake" if create_table else ""}
    """

    # Push to GitHub
    encoded_dag = base64.b64encode(dag_code.encode()).decode()
    url = f"{GITHUB_API_URL}/repos/{GITHUB_REPO}/contents/dags/{dag_name}.py"
    payload = {"message": f"Add {dag_name}.py", "content": encoded_dag, "branch": GITHUB_BRANCH}
    response = requests.put(url, json=payload, headers={"Authorization": f"Bearer {GITHUB_TOKEN}"})
    if response.status_code in [200, 201]:
        st.success(f"{dag_name}.py pushed to GitHub!")
    else:
        st.error(f"Failed to push {dag_name}.py: {response.status_code} - {response.text}")
