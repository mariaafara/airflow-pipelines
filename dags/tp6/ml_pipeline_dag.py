from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import random
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator

default_args = {
    'start_date': datetime(2023, 10, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# This variable is used to specify the name of the S3 bucket where the data will be stored.
BUCKET_NAME = "airflow_bucket"


def check_s3_key(**kwargs):
    """
    Check if the key exists in the S3 bucket.

    :param kwargs: The context dictionary passed by Airflow to the callable.
    :return: Task ID to run based on the key existence.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    s3_hook = S3Hook(aws_conn_id="minio_conn")
    bucket_name = BUCKET_NAME
    key = "tp6/ml_data/data.csv"  # The S3 key you want to check

    # List objects in the bucket and check if the key exists
    objects = s3_hook.list_keys(bucket_name)
    key_exists = key in objects

    if key_exists:
        return "skip_data_ingestion_task"  # Skip data ingestion task
    else:
        return "process_data"  # Run data ingestion task


def process_data(**context):
    """
    A Python callable that generates 500 random data samples and uploads them to a single CSV file in an S3 bucket.

    :param context: The context dictionary that is passed by Airflow to the callable.
    :return: None
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import tempfile
    import csv

    # Generate 500 data samples
    data_samples = []
    for _ in range(500):
        x = random.randint(1, 100)
        y = x * 2  # Simple supervised learning relationship
        data_samples.append((x, y))
    print(data_samples)
    # Create a temporary file to hold the random data
    temp_file = tempfile.NamedTemporaryFile()

    with open(temp_file.name, 'w') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['x', 'y'])
        for row in data_samples:
            csv_out.writerow(row)

    key = f"tp6/ml_data/data.csv"

    # key = f"/opt/airflow/dags/tp6/data.csv" #lesh he ma meshe
    # local_file_path = "/data/data.csv"
    print(key)
    # Upload the data to the S3 bucket
    s3_hook = S3Hook(aws_conn_id="minio_conn")
    # Upload the file to the S3 bucket
    print(f"bucket {BUCKET_NAME} key {key}")
    s3_hook.load_file(filename=temp_file.name, key=key, bucket_name=BUCKET_NAME)

    print("Done")


with DAG('ml_pipeline_dag', default_args=default_args, schedule_interval=None,
         description='A simple Machine Learning pipeline') as dag:
    # Define a dummy task to skip data ingestion
    skip_data_ingestion_task = DummyOperator(task_id="skip_data_ingestion_task")

    check_s3_key_task = BranchPythonOperator(
        task_id="check_s3_key_task",
        python_callable=check_s3_key,
        provide_context=True,
    )

    data_ingestion_task = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
        provide_context=True,
    )

    train_model_task = DockerOperator(
        task_id=f"train_model",
        image="ml_scripts_image",  # The name of the Docker image to use for the task.
        api_version="1.30",  # The version of the Docker API to use.
        auto_remove=False,  # Whether to automatically remove the container after it completes.
        command=f"python scripts/model_training.py --s3_bucket {BUCKET_NAME} --s3_key tp6/ml_data/data.csv",
        # The command to run inside the container.
        # command="tail -f /dev/null",# he be khali lal container locked krml n2dr nfut 3l container n3ml debug (be samou alive)
        docker_url="tcp://docker-socket-proxy:2375",  # The URL of the Docker daemon to connect to.
        network_mode="docker-compose_default",  # The network mode to use for the container.
        trigger_rule="none_failed",
        environment={
            "AWS_ACCESS_KEY_ID": "{{ conn.minio_conn.login }}",
            "AWS_SECRET_ACCESS_KEY": "{{ conn.minio_conn.password }}",

        }
    )

    # Set up task dependencies
    check_s3_key_task >> [data_ingestion_task, skip_data_ingestion_task] >> train_model_task
