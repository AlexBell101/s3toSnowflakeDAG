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

# Step 1: S3 Configuration
st.header("Step 1: S3 Configuration")
bucket_name = st.text_input("S3 Bucket Name", placeholder="e.g., my-data-bucket")
s3_dest_key = st.text_input("S3 Destination Key", placeholder="e.g., your-s3-prefix/")
s3_connection_id = st.text_input("AWS Connection ID", placeholder="e.g., s3")

# Step 2: Snowflake Configuration
st.header("Step 2: Snowflake Configuration")
snowflake_table = st.text_input("Snowflake Table Name", placeholder="e.g., your_table_name")
snowflake_stage = st.text_input("Snowflake Stage Name", placeholder="e.g., your_stage_name")
snowflake_connection_id = st.text_input("Snowflake Connection ID", placeholder="e.g., snowflake")
create_table = st.checkbox("Create a new Snowflake table", value=False)

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
local_file_path = st.text_input("Local File Path", placeholder="e.g., /path/to/your/local/file.csv")

# Generate Files
if st.button("Generate and Push Astro Project to GitHub"):
    # DAG File
    dag_code = f"""
from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
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

    # Task 1: Create table in Snowflake (if checkbox selected)
    {f'''create_table_task = SnowflakeOperator(
        task_id="create_table",
        sql=f\"\"\"
        CREATE TABLE IF NOT EXISTS {snowflake_table} (
            id INT AUTOINCREMENT,
            data STRING,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        \"\"\",
        snowflake_conn_id="{snowflake_connection_id}",
    )''' if create_table else ''}

    # Task 2: Upload local file to S3
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_file_to_s3",
        filename="{local_file_path}",
        dest_key="{s3_dest_key}",
        dest_bucket_name="{bucket_name}",
        aws_conn_id="{s3_connection_id}",
    )

    # Task 3: Load data from S3 into Snowflake
    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="{snowflake_table}",
        stage="{snowflake_stage}",
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        snowflake_conn_id="{snowflake_connection_id}",
    )

    # Define dependencies
    {f'create_table_task >> ' if create_table else ''}upload_to_s3 >> load_to_snowflake
    """

    # Push to GitHub
    encoded_dag = base64.b64encode(dag_code.encode()).decode()
    url = f"{GITHUB_API_URL}/repos/{GITHUB_REPO}/contents/dags/{dag_name}.py"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    response = requests.put(
        url,
        json={"message": f"Add {dag_name}.py", "content": encoded_dag, "branch": GITHUB_BRANCH},
        headers=headers,
    )
    if response.status_code in [200, 201]:
        st.success(f"{dag_name}.py pushed to GitHub!")
    else:
        st.error(f"Failed to push {dag_name}.py: {response.status_code} - {response.text}")
