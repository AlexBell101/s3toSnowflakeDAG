import streamlit as st
import requests
from datetime import datetime
import base64

# GitHub and Astro Configurations
GITHUB_API_URL = "https://api.github.com"
GITHUB_REPO = "AlexBell101/astro-dags"  # Replace with your GitHub repository
GITHUB_BRANCH = "main"  # Replace with your target branch
GITHUB_TOKEN = st.secrets["GITHUB_TOKEN"]  # Access GitHub token from secrets

# Custom CSS for UI
st.markdown("""
    <style>
        html, body, [class*="css"] {
            font-family: 'Inter', sans-serif;
            color: #32325D;
        }
        h1 { font-weight: 600; color: #0070F3 !important; }
        h2, h3 { font-weight: 500; color: #0070F3 !important; }
    </style>
""", unsafe_allow_html=True)

# Title and Intro
st.title("Astro Project Wizard")
st.write("This wizard generates a complete Astro project for deploying Airflow DAGs with minimal effort. Just fill in the details, and we'll do the rest!")

# S3 Configuration
st.header("Step 1: S3 Configuration")
s3_bucket = st.text_input("S3 Bucket Name", placeholder="e.g., my-data-bucket")
s3_key = st.text_input("S3 File Key", placeholder="e.g., folder/myfile.csv")
aws_conn_id = st.text_input("AWS Connection ID", placeholder="e.g., s3")

# Snowflake Configuration
st.header("Step 2: Snowflake Configuration")
snowflake_table = st.text_input("Snowflake Table Name", placeholder="e.g., my_table")
snowflake_stage = st.text_input("Snowflake Stage Name", placeholder="e.g., my_snowflake_stage")
snowflake_conn_id = st.text_input("Snowflake Connection ID", placeholder="e.g., snowflake")

# Checkbox for Table Creation
create_table = st.checkbox("Create Snowflake Table", value=True)
table_columns = ""
if create_table:
    st.write("Define table columns (e.g., `column_name data_type`):")
    table_columns = st.text_area("Table Columns", value="""company_name STRING,
company_domain STRING,
funnel_stage STRING,
total_events INT,
total_events_mom FLOAT,
total_events_wow FLOAT,
unique_sources INT,
unique_sources_mom FLOAT,
unique_sources_wow FLOAT,
total_downloads INT,
total_views INT,
first_seen DATE,
last_seen DATE,
company_linkedin_url STRING,
company_industry STRING,
company_size STRING,
company_country STRING,
company_state STRING,
interest_start_date DATE,
investigation_start_date DATE,
experimentation_start_date DATE,
ongoing_usage_start_date DATE,
inactive_start_date DATE,
unique_total_downloads INT,
unique_total_views INT,
trend FLOAT""")

# DAG Configuration
st.header("Step 3: DAG Configuration")
dag_name = st.text_input("DAG Name", placeholder="e.g., s3_to_snowflake_dag")
schedule_interval = st.text_input("Schedule Interval", value="@daily", placeholder="e.g., @daily")
start_date = st.date_input("Start Date", value=datetime.now())

# Generate and Push DAG to GitHub
if st.button("Generate and Push DAG"):
    # Create the DAG file
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {snowflake_table} ({table_columns})" if create_table else ""
    dag_code = f"""
from airflow import DAG
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
    schedule_interval="{schedule_interval}",
    start_date=datetime({start_date.year}, {start_date.month}, {start_date.day}),
    catchup=False,
) as dag:

    # Task: Create Snowflake Table
    {"create_table_task = SnowflakeOperator(task_id='create_table', sql='''" + create_table_sql + "''', snowflake_conn_id='" + snowflake_conn_id + "')\n" if create_table else ""}

    # Task: Load Data into Snowflake
    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="{snowflake_table}",
        stage="{snowflake_stage}",
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        snowflake_conn_id="{snowflake_conn_id}",
        s3_key="{s3_key}",
        s3_bucket_name="{s3_bucket}",
        aws_conn_id="{aws_conn_id}",
    )

    # Task Dependencies
    {"create_table_task >> load_to_snowflake" if create_table else ""}
    """

    # Push DAG to GitHub
    encoded_dag = base64.b64encode(dag_code.encode()).decode()
    url = f"{GITHUB_API_URL}/repos/{GITHUB_REPO}/contents/dags/{dag_name}.py"
    payload = {"message": f"Add {dag_name}.py", "content": encoded_dag, "branch": GITHUB_BRANCH}
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    response = requests.put(url, json=payload, headers=headers)

    if response.status_code in [200, 201]:
        st.success(f"{dag_name}.py pushed to GitHub!")
    else:
        st.error(f"Failed to push {dag_name}.py: {response.status_code} - {response.text}")
