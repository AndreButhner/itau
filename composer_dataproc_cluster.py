import datetime
import os

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# Path to PySpark script location on Storage.
ingest_tbl_fact_transaction_path = ('gs://my-gcp-project/ingest_tbl_fact_transaction.py')

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project')
}

with models.DAG(
        'composer_dataproc',
        # Continue to run DAG once per day
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

# Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        project_id= models.Variable.get('gcp_project'),
        service_account='38941037693-compute@developer.gserviceaccount.com',
        subnetwork_uri='projects/regions/us-east1/subnetworks/gcp-apps',
        cluster_name='composer-cluster',
        num_workers=2,
        region='us-east1',
        zone='us-east1-b',
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1')

# Run the job on Cloud Dataproc cluster.
    run_dataproc_pyspark = dataproc_operator.DataProcPySparkOperator(
        task_id='run_dataproc_pyspark',
        region='us-east1',
        main=ingest_tbl_fact_transaction_path,
        cluster_name='composer-cluster',
        arguments=None,
        archives=None,
        pyfiles=None,
        files=None,
        dataproc_pyspark_properties=None,
        dataproc_pyspark_jars='gs://spark-lib/bigquery/spark-bigquery-latest.jar')


# Delete Cloud Dataproc cluster.
    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        project_id= models.Variable.get('gcp_project'),
        cluster_name='composer-cluster',
        region='us-east1',
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    # Define DAG dependencies.
    create_dataproc_cluster >> run_dataproc_pyspark >> delete_dataproc_cluster