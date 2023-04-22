import random
from time import sleep

import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator

# This variable is used to specify the name of the S3 bucket where the data will be stored.
BUCKET_NAME = "airflow_bucket"


def process_data(**context):
    """
    A Python callable that generates random data and uploads it to an S3 bucket.

    :param context: The context dictionary that is passed by Airflow to the callable.
    :return: None
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import tempfile

    wait = random.randint(0, 60)
    # Instantiate an S3Hook to interact with the S3 bucket
    s3_hook = S3Hook(aws_conn_id="minio_conn")
    # Create a temporary file to hold the random data
    temp_file = tempfile.NamedTemporaryFile()
    # Generate a random number between 100 and 1000 and write it to the file
    data = random.randint(100, 1000)
    print(data)
    with open(temp_file.name, "w") as f:
        f.write(str(data))
    # Get the execution date from the context and use it to construct the S3 key
    exec_date: pendulum.datetime = context['execution_date']
    key = f"tp1/my_dataset/{exec_date.year}/{exec_date.month}/{exec_date.day}/{exec_date.hour}/{exec_date.minute}/data.txt"
    # Wait for a random amount of time between 0 and 60 seconds
    sleep(wait)
    # Upload the file to the S3 bucket
    s3_hook.load_file(filename=temp_file.name, key=key, bucket_name=BUCKET_NAME)


with DAG(
        dag_id="dag1",
        start_date=pendulum.datetime(2023, 4, 13, 20, 40, tz="Europe/Paris"),
        schedule_interval="*/5 * * * *",  # every 5 minutes
        catchup=True,
        description="A simple DAG that generates random data and uploads it to an S3 bucket.",
) as dag:
    PythonOperator(
        task_id="process_data",
        python_callable=process_data,
        depends_on_past=True,
    )
