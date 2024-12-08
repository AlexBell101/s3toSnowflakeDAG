import streamlit as st
import requests
from datetime import datetime
import base64  # Import for Base64 encoding

# GitHub Configuration
GITHUB_API_URL = "https://api.github.com"
GITHUB_REPO = "AlexBell101/astro-dags"  # Replace with your GitHub repository name
GITHUB_BRANCH = "main"  # Replace with your target branch
GITHUB_TOKEN = st.secrets["GITHUB_TOKEN"]  # Access GitHub token from secrets

# Custom CSS for styling based on Astronomer Brand Guidelines
st.markdown("""
     <style>
        html, body, [class*="css"]  {
            font-family: 'Inter', sans-serif;
            color: #32325D;
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
    </style>
""", unsafe_allow_html=True)

# Title and Introduction
st.title("Astro S3 to Snowflake")
st.write("**New to Airflow? No problem!** Let’s get you orchestrating your data like a pro.")

# Step 1: S3 Configuration
st.header("Step 1: S3 Configuration")
bucket_name = st.text_input("S3 Bucket Name", placeholder="e.g., my-data-bucket")
prefix = st.text_input("S3 Prefix (Optional)", placeholder="e.g., raw/")

# Step 2: Snowflake Configuration
st.header("Step 2: Snowflake Configuration")
snowflake_account = st.text_input("Snowflake Account Name", placeholder="e.g., xy12345.us-east-1")
database = st.text_input("Snowflake Database", placeholder="e.g., analytics")
schema = st.text_input("Snowflake Schema", placeholder="e.g., public")
warehouse = st.text_input("Snowflake Warehouse", placeholder="e.g., compute_wh")
role = st.text_input("Snowflake Role (Optional)", placeholder="e.g., sysadmin")

# Step 3: DAG Configuration
st.header("Step 3: DAG Configuration")
dag_name = st.text_input("DAG Name", placeholder="e.g., s3_to_snowflake_dag")

# Predefined schedule intervals
schedule_options = {
    "Every day at midnight": "@daily",
    "Every hour": "@hourly",
    "Every Monday at 9 AM": "0 9 * * 1",
    "Every 15 minutes": "*/15 * * * *",
    "Every 1st of the month at midnight": "0 0 1 * *",
    "Custom (Advanced)": None
}
selected_schedule = st.selectbox("Select Schedule Interval", list(schedule_options.keys()))
schedule = st.text_input("Enter Custom Schedule Interval") if selected_schedule == "Custom (Advanced)" else schedule_options[selected_schedule]
start_date = st.date_input("Start Date", value=datetime.now())

# Deploy DAG to GitHub
if st.button("Generate and Push DAG to GitHub"):
    # Updated DAG Template
    dag_code = f"""
from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_snowflake import S3ToSnowflakeOperator
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
    description="DAG to transfer data from S3 to Snowflake",
    schedule_interval="{schedule}",
    start_date=datetime({start_date.year}, {start_date.month}, {start_date.day}),
    catchup=False,
) as dag:

    s3_to_snowflake = S3ToSnowflakeOperator(
        task_id="s3_to_snowflake",
        s3_bucket="{bucket_name}",
        s3_key="{prefix}",
        snowflake_conn_id="snowflake_default",
        stage="{schema}.{dag_name}_stage",
        file_format="(type=csv, field_delimiter=',', skip_header=1)",
    )
    """

    # Encode the DAG content to Base64
    encoded_content = base64.b64encode(dag_code.encode()).decode()

    # GitHub API request to upload the file
    file_path = f"dags/{dag_name}.py"
    url = f"{GITHUB_API_URL}/repos/{GITHUB_REPO}/contents/{file_path}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Content-Type": "application/json"
    }

    # Check if file exists and get its SHA
    response = requests.get(url, headers=headers)
    sha = response.json().get("sha") if response.status_code == 200 else None

    # Prepare payload
    payload = {
        "message": f"Add DAG {dag_name}",
        "content": encoded_content,
        "branch": GITHUB_BRANCH
    }
    if sha:
        payload["sha"] = sha

    # Upload the file to GitHub
    response = requests.put(url, json=payload, headers=headers)

    if response.status_code in [200, 201]:
        st.success(f"DAG '{dag_name}' successfully pushed to GitHub!")
    else:
        st.error(f"Failed to push DAG to GitHub: {response.status_code} - {response.text}")
