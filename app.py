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
st.write("Welcome! This wizard will help you create an Astro project with ease, even if you're new to Airflow.")
st.write("**Important**: Before proceeding, ensure your S3 and Snowflake connections are created in Astro.")

# Step 1: S3 Configuration
st.header("Step 1: S3 Configuration")
s3_connection_id = st.text_input("S3 Connection ID", placeholder="e.g., s3_default")
bucket_name = st.text_input("S3 Bucket Name", placeholder="e.g., my-data-bucket")
local_file_path = st.text_input("Local File Path", placeholder="/path/to/your/local/file.csv")
s3_dest_key = st.text_input("S3 Destination Key (Path in Bucket)", placeholder="e.g., Scarf/your_file_name.csv")

st.write("""
**Instructions**:  
1. Your **S3 Connection ID** should match the one created in Astro for AWS.  
2. Your **Local File Path** must be the absolute path to the file you want to upload to S3.  
3. Your **S3 Destination Key** is the path inside the bucket, including folders and file name. For example, `Scarf/my_file.csv`.
""")

# Step 2: Snowflake Configuration
st.header("Step 2: Snowflake Configuration")
snowflake_connection_id = st.text_input("Snowflake Connection ID", placeholder="e.g., snowflake_default")
snowflake_table = st.text_input("Snowflake Table Name", placeholder="e.g., analytics_table")
snowflake_stage = st.text_input("Snowflake Stage Name", placeholder="e.g., your_stage_name")

st.write("""
**Instructions**:  
1. Your **Snowflake Connection ID** should match the one created in Astro for Snowflake.  
2. The **Snowflake Table Name** must either already exist or be created dynamically by the DAG.  
3. Your **Snowflake Stage Name** must point to the correct external stage for the data.
""")

# Step 3: DAG Configuration
st.header("Step 3: DAG Configuration")
dag_name = st.text_input("DAG Name", placeholder="e.g., s3_to_snowflake_dag")
schedule = st.selectbox(
    "How often should this DAG run?",
    ["Daily", "Hourly", "Weekly", "Monthly", "Custom"]
)
if schedule == "Custom":
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
        filename="{local_file_path}",
        dest_key="{s3_dest_key}",
        dest_bucket_name="{bucket_name}",
        aws_conn_id="{s3_connection_id}",
    )

    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="{snowflake_table}",
        stage="{snowflake_stage}",
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        snowflake_conn_id="{snowflake_connection_id}",
    )

    upload_to_s3 >> load_to_snowflake
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
