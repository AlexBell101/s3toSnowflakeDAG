import streamlit as st
import requests
from datetime import datetime

# GitHub Configuration
GITHUB_API_URL = "https://api.github.com"
GITHUB_REPO = "AlexBell101/astro-dags"  # Replace with your GitHub repository name
GITHUB_BRANCH = "main"  # Replace with your target branch
GITHUB_TOKEN = st.secrets["GITHUB_TOKEN"]  # Access GitHub token from secrets

# Custom CSS for styling based on Astronomer Brand Guidelines
st.markdown("""
     <style>
        /* General styles */
        html, body, [class*="css"]  {
            font-family: 'Inter', sans-serif;
            color: #32325D; /* Moon 500 */
        }
        
        /* Title (h1) */
        h1 {
            font-weight: 600; /* Inter Semi Bold */
            color: #0070F3 !important; /* Ensure the color is applied */
        }

        /* Subheaders (h2, h3) */
        h2, h3 {
            font-weight: 500; /* Inter Medium */
            color: #0070F3 !important;
        }

        /* Input text and widgets */
        .stTextInput > label, .stNumberInput > label, .stDateInput > label {
            font-weight: 400; /* Inter Normal */
            color: #A276FF; /* Midnight 500 */
        }
        
        /* Buttons */
        div.stButton > button {
            background-color: #2B6CB0; /* Sapphire 500 */
            color: #FFFFFF; /* White */
            border: None;
            border-radius: 4px;
            padding: 8px 16px;
            font-weight: 500;
        }

        div.stButton > button:hover {
            background-color: #3182CE; /* Sapphire 600 */
        }

        /* Highlight boxes */
        .stMarkdown h3 {
            background-color: #EDF2F7; /* Moon 300 */
            color: #1A202C; /* Midnight 700 */
            padding: 8px;
            border-radius: 4px;
        }
    </style>
""", unsafe_allow_html=True)

# Title and Introduction
st.title("Astro S3 to Snowflake")
st.write("**New to Airflow? No problem!** Let’s get you orchestrating your data like a pro. Moving data from S3 to Snowflake is one of the most common and powerful use cases, and we’ve made it easy for you. Just follow the steps below, and in a few minutes, you’ll have a fully functional Airflow workflow ready to go!")

# Step 1: S3 Configuration
st.header("Step 1: S3 Configuration")
bucket_name = st.text_input("S3 Bucket Name", placeholder="e.g., my-data-bucket")
prefix = st.text_input("S3 Prefix (Optional)", placeholder="e.g., raw/")
aws_access_key = st.text_input("AWS Access Key", type="password", placeholder="Your AWS Access Key")
aws_secret_key = st.text_input("AWS Secret Key", type="password", placeholder="Your AWS Secret Key")

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

# Predefined schedule intervals
schedule_options = {
    "Every day at midnight": "@daily",
    "Every hour": "@hourly",
    "Every Monday at 9 AM": "0 9 * * 1",
    "Every 15 minutes": "*/15 * * * *",
    "Every 1st of the month at midnight": "0 0 1 * *",
    "Custom (Advanced)": None  # Placeholder for custom input
}

# Dropdown for schedule interval
selected_schedule = st.selectbox(
    "Select Schedule Interval",
    list(schedule_options.keys()),
    help="Choose how often the DAG should run. Select 'Custom' if you have a specific cron expression."
)

# If "Custom (Advanced)" is selected, show a text input for custom cron expression
if selected_schedule == "Custom (Advanced)":
    custom_interval = st.text_input(
        "Enter Custom Interval",
        placeholder="e.g., 0 12 * * * for daily at noon",
        help="Provide a valid cron expression or Airflow preset like @daily."
    )
    schedule = custom_interval
else:
    schedule = schedule_options[selected_schedule]

# Show plain-English description of the selected interval
if schedule:
    st.markdown(f"**Selected Interval:** `{schedule}`")

start_date = st.date_input("Start Date", value=datetime.now())

# Deploy DAG to GitHub
if st.button("Generate and Push DAG to GitHub"):
    # DAG Template
    dag_code = f"""
from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from datetime import datetime

with DAG(
    dag_id="{dag_name}",
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
        file_format="(type=csv)",  # Adjust as needed
    )

    s3_to_snowflake
    """

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
        "content": dag_code.encode("utf-8").decode("utf-8"),
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
