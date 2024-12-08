import streamlit as st
import requests
from datetime import datetime
import base64

# GitHub Configuration
GITHUB_API_URL = "https://api.github.com"
GITHUB_REPO = "AlexBell101/astro-dags"  # Replace with your GitHub repository name
GITHUB_BRANCH = "main"  # Replace with your target branch
GITHUB_TOKEN = st.secrets["GITHUB_TOKEN"]  # Access GitHub token from secrets
ASTRO_API_URL = st.secrets["ASTRO_API"]  # Access Astro API URL from secrets
ASTRO_API_TOKEN = st.secrets["API"]  # Access Astro API token from secrets

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
st.write("Welcome! This wizard will help you create an Astro project with ease, even if you're new to Airflow.")

# Step 1: S3 Configuration
st.header("Step 1: S3 Configuration")
bucket_name = st.text_input("S3 Bucket Name", placeholder="e.g., my-data-bucket")
prefix = st.text_input("S3 Prefix (Optional)", placeholder="e.g., raw/")
aws_access_key = st.text_input("AWS Access Key ID", type="password")
aws_secret_key = st.text_input("AWS Secret Access Key", type="password")

# Step 2: Snowflake Configuration
st.header("Step 2: Snowflake Configuration")
snowflake_account = st.text_input("Snowflake Account Name", placeholder="e.g., xy12345.us-east-1")
database = st.text_input("Snowflake Database", placeholder="e.g., analytics")
schema = st.text_input("Snowflake Schema", placeholder="e.g., public")
warehouse = st.text_input("Snowflake Warehouse", placeholder="e.g., compute_wh")
role = st.text_input("Snowflake Role (Optional)", placeholder="e.g., sysadmin")
username = st.text_input("Snowflake Username", placeholder="Your Snowflake Username")
password = st.text_input("Snowflake Password", type="password", placeholder="Your Snowflake Password")

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

# Generate Astro Connections via API
if st.button("Set Up Connections in Astro"):
    # Create S3 Connection
    s3_payload = {
        "connection_id": "aws_default",
        "conn_type": "aws",
        "extra": f'{{"aws_access_key_id": "{aws_access_key}", "aws_secret_access_key": "{aws_secret_key}"}}'
    }
    snowflake_payload = {
        "connection_id": "snowflake_default",
        "conn_type": "snowflake",
        "host": snowflake_account,
        "schema": schema,
        "login": username,
        "password": password,
        "extra": f'{{"database": "{database}", "warehouse": "{warehouse}", "role": "{role}"}}'
    }
    headers = {"Authorization": f"Bearer {ASTRO_API_TOKEN}"}
    s3_response = requests.post(f"{ASTRO_API_URL}/api/v1/connections", json=s3_payload, headers=headers)
    snowflake_response = requests.post(f"{ASTRO_API_URL}/api/v1/connections", json=snowflake_payload, headers=headers)
    if s3_response.status_code == 201:
        st.success("S3 connection added to Astro!")
    else:
        st.error(f"Failed to add S3 connection: {s3_response.json()}")
    if snowflake_response.status_code == 201:
        st.success("Snowflake connection added to Astro!")
    else:
        st.error(f"Failed to add Snowflake connection: {snowflake_response.json()}")

# Generate Files
if st.button("Generate and Push Astro Project to GitHub"):
    # DAG File
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

    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_file_to_s3",
        filename="/path/to/your/local/file.csv",
        dest_key="{prefix}",
        dest_bucket_name="{bucket_name}",
        aws_conn_id="aws_default",
    )

    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="your_table_name",
        stage="{dag_name}_stage",
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        snowflake_conn_id="snowflake_default",
    )

    upload_to_s3 >> load_to_snowflake
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
