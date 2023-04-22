import pendulum
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

BUCKET_NAME = "airflow_bucket"
KEY_NAME = "tp1/my_dataset/{{ execution_date.year }}/{{ execution_date.month }}/{{ execution_date.day }}/{{ execution_date.hour }}/{{ execution_date.minute }}/data.txt"


def load_data(key, **context):
    """
    A Python callable that downloads and prints the contents of a file from an S3 bucket.

    :param key: The key of the file in the S3 bucket.
    :param context: The context dictionary that is passed by Airflow to the callable.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    s3_hook = S3Hook(aws_conn_id="minio_conn")
    file_name = s3_hook.download_file(key=key, bucket_name=BUCKET_NAME, local_path="/tmp")
    with open(file_name) as f:
        print(f.read())


with DAG(
        dag_id="dag2",
        start_date=pendulum.datetime(2023, 4, 13, 20, 40, tz="Europe/Paris"),
        schedule_interval="*/5 * * * *",  # every 5 minutes
        catchup=True,
        description="A simple DAG that downloads and prints the contents of a file from an S3 bucket.",
) as dag:
    """ This task waits for the S3 key to be available before starting the next task.
    By default, the S3KeySensor task will wait indefinitely until the S3 key becomes available. 
    The poke_interval parameter is used to specify the interval at which the task should check for the key. 
    The timeout parameter is used to specify the maximum amount of time the task should wait before failing.
    """
    s3_sensor = S3KeySensor(
        task_id="s3_sensor",
        bucket_name=BUCKET_NAME,  # The name of the S3 bucket to check for the key
        bucket_key=KEY_NAME,  # The key of the S3 object to check for.
        aws_conn_id="minio_conn",  # The name of the Airflow connection to use to connect to the S3 service.
        mode="reschedule",
        # The mode in which the sensor should operate. In this case, it is set to "reschedule",
        # which means that the sensor will keep checking for the key until it becomes available.
        poke_interval=5,  # The interval at which the sensor should check for the key, in seconds.
        depends_on_past=True,
        # Whether or not this task instance should depend on the success or failure of the previous task instance.
    )
    """This task downloads and prints the contents of a file from an S3 bucket."""
    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        op_kwargs={"key": KEY_NAME},
        depends_on_past=True,
    )
    # Set the dependencies between tasks
    s3_sensor >> load_data
