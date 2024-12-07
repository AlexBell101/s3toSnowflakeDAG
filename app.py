import streamlit as st
from datetime import datetime

# Title and Introduction
st.title("Astro S3 to Snowflake")
st.write("Welcome to the Astro S3 to Snowflake wizard! This tool will help you generate an Airflow DAG to move data from S3 to Snowflake with ease.")

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
schedule = st.text_input("Schedule Interval", placeholder="e.g., @daily or 0 12 * * *")
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
