"""
This configures a weekly DAG to run the TAAR Ensemble job off.
"""
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from datetime import datetime, timedelta
from airflow.operators.subdag_operator import SubDagOperator

from operators.gcp_container_operator import GKEPodOperator  # noqa
from utils.dataproc import moz_dataproc_pyspark_runner

taar_aws_conn_id = "airflow_taar_rw_s3"
taar_aws_access_key, taar_aws_secret_key, session = AwsHook(
    taar_aws_conn_id
).get_credentials()
taar_ensemble_cluster_name = "dataproc-taar-ensemble"
taar_gcpdataproc_conn_id = "google_cloud_airflow_dataproc"

taar_bigquery_svc_conn_id = "google_cloud_cfr_svc"

TAAR_PROFILE_PROJECT_ID = "cfr-personalization-experiment"

TAAR_BIGTABLE_IMAGE = f"gcr.io/{TAAR_PROFILE_PROJECT_ID}/taar_gcp_etl:0.3"


default_args_weekly = {
    "owner": "vng@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2020, 4, 4),
    "email": ["telemetry-alerts@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=60),
}


taar_weekly = DAG(
    "taar_weekly", default_args=default_args_weekly, schedule_interval="@weekly"
)


def wipe_args():
    return [
        "gsutil",
        "-m",
        "rm",
        "gs://taar_profile_dump/*",
    ]


wipe_gcs_bucket = GKEPodOperator(
    task_id="wipe_taar_gcs_bucket",
    gcp_conn_id=taar_bigquery_svc_conn_id,
    project_id=TAAR_PROFILE_PROJECT_ID,
    location="us-central1-a",
    cluster_name="bq-load-gke-1",
    name="wipe_taar_gcs_bucket",
    namespace="default",
    image="google/cloud-sdk:242.0.0-alpine",
    arguments=wipe_args(),
    dag=taar_weekly,
)

dump_bq_to_tmp_table = GKEPodOperator(
    task_id="taar_profile_bqdump",
    name="taar-profile-bqdump",
    # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
    image=TAAR_BIGTABLE_IMAGE,
    project_id=TAAR_PROFILE_PROJECT_ID,
    owner="vng@mozilla.com",
    email=["vng@mozilla.com",],
    gcp_conn_id=taar_bigquery_svc_conn_id,
    arguments=[
        "-m",
        "taar_etl.taar_profile_bigtable",
        "--iso-date={{ ds_nodash }}",
        "--fill-table",
    ],
    dag=taar_weekly,
)

extract_bq_tmp_to_gcs_avro = GKEPodOperator(
    task_id="taar_profile_bqdump",
    name="taar-profile-bqdump",
    # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
    image=TAAR_BIGTABLE_IMAGE,
    project_id=TAAR_PROFILE_PROJECT_ID,
    owner="vng@mozilla.com",
    email=["vng@mozilla.com",],
    gcp_conn_id=taar_bigquery_svc_conn_id,
    arguments=[
        "-m",
        "taar_etl.taar_profile_bigtable",
        "--iso-date={{ ds_nodash }}",
        "--import-bigtable",
    ],
    dag=taar_weekly,
)

dataflow_import_avro_to_bigtable = GKEPodOperator(
    task_id="taar_profile_bqdump",
    name="taar-profile-bqdump",
    # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
    image=TAAR_BIGTABLE_IMAGE,
    project_id=TAAR_PROFILE_PROJECT_ID,
    owner="vng@mozilla.com",
    email=["vng@mozilla.com",],
    gcp_conn_id=taar_bigquery_svc_conn_id,
    arguments=[
        "-m",
        "taar_etl.taar_profile_bigtable",
        "--iso-date={{ ds_nodash }}",
        "--extract-avro",
    ],
    dag=taar_weekly,
)

wipe_gcs_bucket_cleanup = GKEPodOperator(
    task_id="wipe_taar_gcs_bucket_cleanup",
    gcp_conn_id=taar_bigquery_svc_conn_id,
    project_id=TAAR_PROFILE_PROJECT_ID,
    location="us-central1-a",
    cluster_name="bq-load-gke-1",
    name="wipe_taar_gcs_bucket",
    namespace="default",
    image="google/cloud-sdk:242.0.0-alpine",
    arguments=wipe_args(),
    dag=taar_weekly,
)

wipe_bq_tmp_table = GKEPodOperator(
    task_id="taar_profile_wipe_bq_tmp_table",
    name="taar-profile-wipe-bq-tmp",
    # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
    image=TAAR_BIGTABLE_IMAGE,
    project_id=TAAR_PROFILE_PROJECT_ID,
    owner="vng@mozilla.com",
    email=["vng@mozilla.com",],
    gcp_conn_id=taar_bigquery_svc_conn_id,
    arguments=[
        "-m",
        "taar_etl.taar_profile_bigtable",
        "--iso-date={{ ds_nodash }}",
        "--wipe-bg-tmp-table",
    ],
    dag=taar_weekly,
)



# This job should complete in approximately 30 minutes given
# 35 x n1-standard-8 workers and 2 SSDs per node.
taar_ensemble = SubDagOperator(
    task_id="taar_ensemble",
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=taar_weekly.dag_id,
        dag_name="taar_ensemble",
        default_args=default_args_weekly,
        cluster_name=taar_ensemble_cluster_name,
        job_name="TAAR_ensemble",
        python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/taar_ensemble.py",
        additional_properties={
            "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest.jar",
            "spark:spark.hadoop.fs.s3a.access.key": taar_aws_access_key,
            "spark:spark.hadoop.fs.s3a.secret.key": taar_aws_secret_key,
            "spark:spark.jars.packages": "org.apache.spark:spark-avro_2.11:2.4.4",
            "spark:spark.python.profile": "true",
        },
        num_workers=35,
        worker_machine_type="n1-standard-8",
        master_machine_type="n1-standard-8",
        init_actions_uris=["gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/pip-install.sh"],
        additional_metadata={
            "PIP_PACKAGES": "mozilla-taar3==0.4.12 mozilla-srgutil==0.2.1 python-decouple==3.1 click==7.0 boto3==1.7.71 dockerflow==2018.4.0"
        },
        optional_components=["ANACONDA", "JUPYTER"],
        py_args=[
            "--date",
            "{{ ds_nodash }}",
            "--aws_access_key_id",
            taar_aws_access_key,
            "--aws_secret_access_key",
            taar_aws_secret_key,
            "--sample_rate",
            "0.005",
        ],
        aws_conn_id=taar_aws_conn_id,
        gcp_conn_id=taar_gcpdataproc_conn_id,
        master_disk_type="pd-ssd",
        worker_disk_type="pd-ssd",
        master_disk_size=1024,
        worker_disk_size=1024,
        master_num_local_ssds=2,
        worker_num_local_ssds=2,
    ),
    dag=taar_weekly,
)



wipe_gcs_bucket >> dump_bq_to_tmp_table
dump_bq_to_tmp_table >> extract_bq_tmp_to_gcs_avro
extract_bq_tmp_to_gcs_avro >> dataflow_import_avro_to_bigtable
dataflow_import_avro_to_bigtable >> wipe_gcs_bucket_cleanup
wipe_gcs_bucket_cleanup >> wipe_bq_tmp_table
wipe_bq_tmp_table >> taar_ensemble

