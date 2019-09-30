from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.hooks.aws_hook import AwsHook

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from operators.gcp_container_operator import GKEPodOperator

# Unsalted cluster name so subsequent runs fail if the cluster name exists
cluster_name = "bq-load-gke-1"

# Defined in Airflow's UI -> Admin -> Connections
gcp_conn_id = "google_cloud_derived_datasets_3"
connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

aws_conn_id = "airflow_taar_rw_s3"
aws_access_key, aws_secret_key, session = AwsHook(aws_conn_id).get_credentials()

default_args = {
    "owner": "vng@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 9, 30),
    "email": ["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG("taar_amodump", default_args=default_args, schedule_interval="@daily")

amodump = GKEPodOperator(
    task_id="taar_amodump",
    gcp_conn_id=gcp_conn_id,
    job_name="Dump AMO JSON blobs with oldest creation date per addon.",
    project_id=connection.project_id,
    location="us-central1-a",
    cluster_name="bq-load-gke-1",
    name="taar-amodump",
    namespace="default",
    image="gcr.io/moz-fx-data-airflow-prod-88e0/taar_gcp_etl:0.1",
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com", "hwoo@mozilla.com"],
    arguments=["-m", "taar_etl.taar_amodump", "--date", "{{ ds_nodash }}"],
    env_vars={
        "AWS_ACCESS_KEY_ID": aws_access_key,
        "AWS_SECRET_ACCESS_KEY": aws_secret_key,
    },
    dag=dag,
)

amowhitelist = GKEPodOperator(
    task_id="taar_amowhitelist",
    gcp_conn_id=gcp_conn_id,
    job_name="Generate an algorithmically defined set of whitelisted addons for TAAR",
    project_id=connection.project_id,
    location="us-central1-a",
    cluster_name="bq-load-gke-1",
    name="taar-amowhitelist",
    namespace="default",
    image="gcr.io/moz-fx-data-airflow-prod-88e0/taar_gcp_etl:0.1",
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com", "hwoo@mozilla.com"],
    arguments=["-m", "taar_etl.taar_amowhitelist"],
    env_vars={
        "AWS_ACCESS_KEY_ID": aws_access_key,
        "AWS_SECRET_ACCESS_KEY": aws_secret_key,
    },
    dag=dag,
)

editorial_whitelist = GKEPodOperator(
    task_id="taar_update_whitelist",
    job_name="Generate a JSON blob from editorial reviewed addons for TAAR",
    project_id=connection.project_id,
    location="us-central1-a",
    cluster_name="bq-load-gke-1",
    name="taar-update-whitelist",
    namespace="default",
    image="gcr.io/moz-fx-data-airflow-prod-88e0/taar_gcp_etl:0.1",
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com", "hwoo@mozilla.com"],
    arguments=["-m", "taar_etl.taar_update_whitelist", "--date", "{{ ds_nodash }}"],
    env_vars={
        "AWS_ACCESS_KEY_ID": aws_access_key,
        "AWS_SECRET_ACCESS_KEY": aws_secret_key,
    },
    dag=dag,
)
