import streamlit as st
from datetime import datetime

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

# Generate DAG Code
if st.button("Generate DAG"):
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
    
    st.code(dag_code, language="python")

    # Provide Download and Next Steps
    st.download_button(
        label="Download DAG File",
        data=dag_code,
        file_name=f"{dag_name}.py",
        mime="text/x-python"
    )

    # Next Steps Instructions
    st.markdown("""
    ### Next Steps
    1. Save the downloaded file in the `dags/` folder of your Astronomer project.
    2. Open your terminal and navigate to your Astronomer project directory.
    3. Run the following commands to start your local Airflow instance:
       ```bash
       astro dev start
       ```
    4. Access the Airflow UI at `http://localhost:8080` to see your new DAG!
    5. Trigger the DAG to start moving data from S3 to Snowflake.

    For more details, check out the [Astronomer CLI Docs](https://www.astronomer.io/docs/astro/cli/develop-project).
    """)
