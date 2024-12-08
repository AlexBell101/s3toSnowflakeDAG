import streamlit as st
import requests
from datetime import datetime
import base64

# GitHub Configuration
GITHUB_API_URL = "https://api.github.com"
GITHUB_REPO = "AlexBell101/astro-dags"  # Replace with your GitHub repository name
GITHUB_BRANCH = "main"  # Replace with your target branch
GITHUB_TOKEN = st.secrets["GITHUB_TOKEN"]  # Access GitHub token from secrets

# Custom CSS for styling
st.markdown("""
    <style>
        html, body, [class*="css"] { font-family: 'Inter', sans-serif; color: #32325D; }
        h1 { font-weight: 600; color: #0070F3 !important; }
        h2, h3 { font-weight: 500; color: #0070F3 !important; }
        div.stButton > button { background-color: #2B6CB0; color: #FFFFFF; padding: 8px 16px; }
    </style>
""", unsafe_allow_html=True)

# Title
st.title("Astro Wizard: S3 to Snowflake")
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

# Step 3: Operator Logic
st.header("Step 3: Operator Logic")
use_s3_to_snowflake = st.checkbox("Use S3ToSnowflakeOperator (Default)", value=True)

# Generate and Push Astro Project
if st.button("Generate and Push Astro Project"):
    # Create DAG File
    if use_s3_to_snowflake:
        dag_code = f"""
try:
    from airflow.providers.amazon.aws.transfers.s3_to_snowflake import S3ToSnowflakeOperator
except ImportError:
    from airflow.providers.amazon.aws.operators.s3_to_snowflake import S3ToSnowflakeOperator

from airflow import DAG
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
        stage="{dag_name}_stage",
        file_format="(type=csv, field_delimiter=',', skip_header=1)",
    )
    """
    else:
        dag_code = f"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def custom_s3_to_snowflake(**kwargs):
    # Add your custom logic here
    print("Transferring data from S3 to Snowflake")

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

    custom_task = PythonOperator(
        task_id="s3_to_snowflake_custom",
        python_callable=custom_s3_to_snowflake,
    )
    """

    # Push to GitHub
    files_to_push = {
        f"dags/{dag_name}.py": dag_code,
        "requirements.txt": dependencies,
    }
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    for file_path, content in files_to_push.items():
        url = f"{GITHUB_API_URL}/repos/{GITHUB_REPO}/contents/{file_path}"
        encoded_content = base64.b64encode(content.encode()).decode()
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
