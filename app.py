import streamlit as st
import requests
from datetime import datetime
import base64
import json

# GitHub Configuration
GITHUB_API_URL = "https://api.github.com"
GITHUB_REPO = "AlexBell101/astro-dags"  # Replace with your GitHub repository name
GITHUB_BRANCH = "main"  # Replace with your target branch
GITHUB_TOKEN = st.secrets["GITHUB_TOKEN"]  # Access GitHub token from secrets

# Astro API Configuration
ASTRO_API_URL = "https://cloud.astronomer.io/hub/v1"  # Replace with the Astro API base URL
ASTRO_API_TOKEN = st.secrets["API"]  # Access Astro API token from secrets
WORKSPACE_ID = "your-workspace-id"  # Replace with your Astro workspace ID
DEPLOYMENT_ID = "your-deployment-id"  # Replace with your Astro deployment ID

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

# Introduction
st.write("""
### Welcome to the Astro Project Wizard!
Whether you're a seasoned Airflow pro or brand new to the platform, you're in the right place. This wizard will guide you step-by-step in creating a complete Astro project to deploy your DAGs, integrate with Snowflake, and set up connections automatically.

No prior experience? No problem! We've designed this wizard to make everything simple and intuitive. Let's get started! 🚀
""")

# Step 1: DAG Configuration
st.header("Step 1: Configure Your DAG")
dag_name = st.text_input("DAG Name", placeholder="e.g., s3_to_snowflake_dag")
schedule = st.selectbox(
    "DAG Schedule Interval",
    ["@daily", "@hourly", "0 9 * * 1", "*/15 * * * *", "Custom (Advanced)"]
)
if schedule == "Custom (Advanced)":
    schedule = st.text_input("Custom Schedule Interval", placeholder="e.g., 0 12 * * *")
start_date = st.date_input("Start Date", value=datetime.now())

# Step 2: S3 Configuration
st.header("Step 2: Configure S3 Inputs")
bucket_name = st.text_input("S3 Bucket Name", placeholder="e.g., my-data-bucket")
prefix = st.text_input("S3 Prefix (Optional)", placeholder="e.g., raw/")

# Step 3: Snowflake Configuration
st.header("Step 3: Snowflake Configuration")
snowflake_account = st.text_input("Snowflake Account Name", placeholder="e.g., xy12345.us-east-1")
database = st.text_input("Snowflake Database", placeholder="e.g., analytics")
schema = st.text_input("Snowflake Schema", placeholder="e.g., public")
warehouse = st.text_input("Snowflake Warehouse", placeholder="e.g., compute_wh")
role = st.text_input("Snowflake Role (Optional)", placeholder="e.g., sysadmin")
username = st.text_input("Snowflake Username", placeholder="Your Snowflake Username")
password = st.text_input("Snowflake Password", type="password", placeholder="Your Snowflake Password")

# Step 4: Dependencies
st.header("Step 4: Define Additional Dependencies")
dependencies = st.text_area(
    "Python Dependencies (Optional)",
    "snowflake-connector-python\napache-airflow-providers-amazon\napache-airflow-providers-snowflake",
)

# Generate Files and Add Astro Connection
if st.button("Generate and Push Astro Project to GitHub and Add Connection"):
    # Create DAG File
    dag_code = f"""
from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
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

    # Task 1: Upload local file to S3
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_file_to_s3",
        filename="/path/to/your/local/file.csv",  # Replace with the local file path
        dest_key="{prefix}",
        dest_bucket_name="{bucket_name}",
        aws_conn_id="aws_default",
    )

    # Task 2: Load data from S3 into Snowflake
    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="your_table_name",  # Replace with your Snowflake table name
        stage="your_stage_name",  # Replace with your Snowflake stage
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        pattern=".*\\.csv",
        snowflake_conn_id="snowflake_default",
    )

    # Task dependencies
    upload_to_s3 >> load_to_snowflake
    """
    # Create Dockerfile
    dockerfile_content = """
FROM quay.io/astronomer/astro-runtime:12.5.0
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
"""
    # Create requirements.txt
    requirements_content = dependencies

    # Prepare files
    files_to_push = {
        f"dags/{dag_name}.py": dag_code,
        "Dockerfile": dockerfile_content,
        "requirements.txt": requirements_content,
    }

    # Push to GitHub
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Content-Type": "application/json"}
    for file_path, file_content in files_to_push.items():
        encoded_content = base64.b64encode(file_content.encode()).decode()
        url = f"{GITHUB_API_URL}/repos/{GITHUB_REPO}/contents/{file_path}"
        response = requests.get(url, headers=headers)
        sha = response.json().get("sha") if response.status_code == 200 else None

        payload = {"message": f"Add {file_path}", "content": encoded_content, "branch": GITHUB_BRANCH}
        if sha:
            payload["sha"] = sha
        response = requests.put(url, json=payload, headers=headers)
        if response.status_code in [200, 201]:
            st.success(f"{file_path} pushed to GitHub!")
        else:
            st.error(f"Failed to push {file_path}: {response.status_code} - {response.text}")

    # Add Snowflake connection to Astro
    astro_headers = {"Authorization": f"Bearer {ASTRO_API_TOKEN}", "Content-Type": "application/json"}
    connection_payload = {
        "connectionId": "snowflake_default",
        "deploymentId": DEPLOYMENT_ID,
        "connectionType": "snowflake",
        "connectionMetadata": {
            "account": snowflake_account,
            "database": database,
            "schema": schema,
            "warehouse": warehouse,
            "role": role,
            "username": username,
            "password": password,
        }
    }
    connection_url = f"{ASTRO_API_URL}/workspaces/{WORKSPACE_ID}/connections"
    response = requests.post(connection_url, json=connection_payload, headers=astro_headers)

    if response.status_code in [200, 201]:
        st.success("Snowflake connection added to Astro successfully!")
    else:
        st.error(f"Failed to add Snowflake connection: {response.status_code} - {response.text}")
