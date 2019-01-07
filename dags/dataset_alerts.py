from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.s3fs_check_success import S3FSCheckSuccessSensor
from airflow.operators.dataset_status import DatasetStatusOperator

default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 2),
    "email": ["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    "email_on_failure": True,
}

# MainSummary can take up to 7 hours, depending on the day.
# Set a sensor to check for the results.
dag = DAG("dataset_alerts", default_args=default_args, schedule_interval="0 1 * * *")

S3FSCheckSuccessSensor(
    task_id="check_main_summary",
    bucket="telemetry-parquet",
    prefix="main_summary/v4/submission_date_s3={{ ds_nodash }}",
    num_partitions=100,
    poke_interval=30 * 60,
    timeout=8 * 60 * 60,
    dag=dag,
) >> DatasetStatusOperator(
    task_id="check_main_summary_failure",
    trigger_rule="all_failed",
    status="partial_outage",
    name="Main Summary",
    description="A summary view of main pings.",
    dag=dag,
)
