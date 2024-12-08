import streamlit as st
import requests
import base64
from datetime import datetime

# GitHub Configuration
GITHUB_API_URL = "https://api.github.com"
GITHUB_REPO = "AlexBell101/astro-dags"
GITHUB_BRANCH = "main"
GITHUB_TOKEN = st.secrets["GITHUB_TOKEN"]

# Custom CSS for styling based on Astronomer Brand Guidelines
st.markdown("""
    <style>
        html, body, [class*="css"]  {
            font-family: 'Inter', sans-serif;
            color: #32325D; /* Moon 500 */
        }
        h1 {
            font-weight: 600;
            color: #0070F3 !important;
        }
        h2, h3 {
            font-weight: 500;
            color: #0070F3 !important;
        }
        .stTextInput > label, .stNumberInput > label, .stDateInput > label {
            font-weight: 400;
            color: #A276FF;
        }
        div.stButton > button {
            background-color: #2B6CB0;
            color: #FFFFFF;
            border: None;
            border-radius: 4px;
            padding: 8px 16px;
            font-weight: 500;
        }
        div.stButton > button:hover {
            background-color: #3182CE;
        }
        .stMarkdown h3 {
            background-color: #EDF2F7;
            color: #1A202C;
            padding: 8px;
            border-radius: 4px;
        }
    </style>
""", unsafe_allow_html=True)

# Title and Introduction
st.title("Astro S3 to Snowflake")
st.write("This wizard generates a complete Astro project for deploying Airflow DAGs.")

# Step 1: Configure Your DAG
st.header("Step 1: Configure Your DAG")
dag_name = st.text_input("DAG Name", placeholder="e.g., s3_to_snowflake_dag")
bucket_name = st.text_input("S3 Bucket Name", placeholder="e.g., my-data-bucket")
prefix = st.text_input("S3 Prefix (Optional)", placeholder="e.g., raw/")
schedule = st.text_input("DAG Schedule Interval", placeholder="@daily")
start_date = st.date_input("Start Date", value=datetime.now())

# Step 2: Define Additional Dependencies
st.header("Step 2: Define Additional Dependencies")
dependencies = st.text_area(
    "Python Dependencies (Optional)",
    "snowflake-connector-python\napache-airflow-providers-amazon\napache-airflow-providers-snowflake",
    help="List additional Python libraries required for your DAG."
)

# Generate and Push Project
if st.button("Generate and Push Project to GitHub"):
    # Generate DAG file
    dag_code = f"""
from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.models import Variable
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
    s3_to_snowflake = S3ToSnowflakeOperator(
        task_id="s3_to_snowflake",
        s3_bucket="{bucket_name}",
        s3_key="{prefix}",
        aws_conn_id="aws_default",
        snowflake_conn_id="snowflake_default",
        stage="public.{dag_name}_stage",
        file_format="(type=csv, field_delimiter=',', skip_header=1)",
    )
    """

    # Generate Dockerfile
    dockerfile_content = "FROM quay.io/astronomer/astro-runtime:7.3.0"

    # Generate requirements.txt
    requirements_content = dependencies

    # Prepare files for GitHub
    files_to_push = {
        f"dags/{dag_name}.py": dag_code,
        "Dockerfile": dockerfile_content,
        "requirements.txt": requirements_content,
    }

    # Push each file to GitHub
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Content-Type": "application/json",
    }
    for file_path, file_content in files_to_push.items():
        encoded_content = base64.b64encode(file_content.encode()).decode()
        url = f"{GITHUB_API_URL}/repos/{GITHUB_REPO}/contents/{file_path}"
        response = requests.get(url, headers=headers)
        sha = response.json().get("sha") if response.status_code == 200 else None

        payload = {
            "message": f"Add {file_path}",
            "content": encoded_content,
            "branch": GITHUB_BRANCH,
        }
        if sha:
            payload["sha"] = sha

        response = requests.put(url, json=payload, headers=headers)
        if response.status_code in [200, 201]:
            st.success(f"{file_path} successfully pushed to GitHub!")
        else:
            st.error(f"Failed to push {file_path}: {response.status_code} - {response.text}")
