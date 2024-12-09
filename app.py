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
st.write("Welcome! This wizard will help you create an Astro project with ease, even if you're new to Airflow. Before proceeding, ensure that you've created your S3 and Snowflake connections in Astro and have their connection IDs handy.")

# Step 1: S3 Configuration
st.header("Step 1: S3 Configuration")
bucket_name = st.text_input("S3 Bucket Name", placeholder="e.g., scarfdata")
s3_key = st.text_input("S3 Key (File Path)", placeholder="e.g., Scarf/data.csv")
aws_connection_id = st.text_input("AWS Connection ID", placeholder="e.g., s3")

# Step 2: Snowflake Configuration
st.header("Step 2: Snowflake Configuration")
snowflake_connection_id = st.text_input("Snowflake Connection ID", placeholder="e.g., snowflake")
snowflake_table = st.text_input("Snowflake Table Name", placeholder="e.g., analytics_data")
snowflake_database = st.text_input("Snowflake Database", placeholder="e.g., analytics_db")
snowflake_schema = st.text_input("Snowflake Schema", placeholder="e.g., public")

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

# Generate and Push DAG to GitHub
if st.button("Generate and Push DAG to GitHub"):
    # DAG File
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
        schema="{snowflake_schema}",
        database="{snowflake_database}",
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        pattern=".*\\.csv",
        snowflake_conn_id="{snowflake_connection_id}",
        s3_key="{s3_key}",
        aws_conn_id="{aws_connection_id}",
    )
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
