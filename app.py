import streamlit as st
from datetime import datetime
import base64
import requests

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
st.write("""
Welcome! This wizard will guide you in creating an Astro project.
Before starting:
1. Go to Astro and create your S3 and Snowflake connections using the **Connections** tab.
2. Note the `connection_id` for each connection.

Once you're ready, fill out the details below and let the wizard handle the rest!
""")

# Step 1: Connection Details
st.header("Step 1: Specify Connection IDs")
s3_connection_id = st.text_input("S3 Connection ID", placeholder="e.g., aws_default")
snowflake_connection_id = st.text_input("Snowflake Connection ID", placeholder="e.g., snowflake_default")

# Step 2: DAG Configuration
st.header("Step 2: Configure Your DAG")
dag_name = st.text_input("DAG Name", placeholder="e.g., s3_to_snowflake_dag")
schedule = st.selectbox(
    "How often should this DAG run?",
    ["Daily", "Hourly", "Weekly", "Custom (Advanced)"]
)
if schedule == "Custom (Advanced)":
    schedule = st.text_input("Custom Schedule Interval", placeholder="e.g., 0 12 * * *")
start_date = st.date_input("Start Date", value=datetime.now())

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
        dest_key="your-s3-prefix/",  # Replace with your S3 key
        dest_bucket_name="your-s3-bucket-name",  # Replace with your S3 bucket
        aws_conn_id="{s3_connection_id}",  # Use the user-provided connection ID for AWS
    )

    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="your_table_name",  # Replace with your Snowflake table name
        stage="your_stage_name",  # Replace with your Snowflake stage
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        snowflake_conn_id="{snowflake_connection_id}",  # Use the user-provided connection ID for Snowflake
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
