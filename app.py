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
st.write("This wizard generates a complete Astro project for deploying Airflow DAGs.")

# Step 1: DAG Configuration
st.header("Step 1: Configure Your DAG")
dag_name = st.text_input("DAG Name", placeholder="e.g., s3_to_snowflake_dag")
bucket_name = st.text_input("S3 Bucket Name", placeholder="e.g., my-data-bucket")
prefix = st.text_input("S3 Prefix (Optional)", placeholder="e.g., raw/")
schedule = st.selectbox(
    "DAG Schedule Interval",
    ["@daily", "@hourly", "0 9 * * 1", "*/15 * * * *", "Custom (Advanced)"]
)
if schedule == "Custom (Advanced)":
    schedule = st.text_input("Custom Schedule Interval", placeholder="e.g., 0 12 * * *")
start_date = st.date_input("Start Date", value=datetime.now())

# Step 2: Dependencies
st.header("Step 2: Define Additional Dependencies")
dependencies = st.text_area(
    "Python Dependencies (Optional)",
    "snowflake-connector-python\napache-airflow-providers-amazon\napache-airflow-providers-snowflake",
)

# Generate Files
if st.button("Generate and Push Astro Project to GitHub"):
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
        stage="{dag_name}_stage",
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
