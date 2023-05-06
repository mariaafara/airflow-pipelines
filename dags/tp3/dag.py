import os

from datetime import datetime
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from docker.types import Mount
from airflow.models import Variable

with DAG(
        'pandas_dag',
        description='A simple DAG to demonstrate how to use DockerOperator.',
        schedule_interval=None,
        start_date=datetime(2023, 5, 1),
        catchup=False
) as dag:
    # Get the project absolute path from Airflow Variables
    project_absolute_path = Variable.get("PROJECT_ABSOLUTE_PATH")
    absolute_data_folder_path = os.path.join(project_absolute_path, "dags/tp3/data")

    # Define the first DockerOperator to run create_df.py
    create_df_op = DockerOperator(
        task_id='create_df',
        image='my_pandas_image',  # The name of the Docker image to use for the task.
        api_version='1.30',  # The version of the Docker API to use.
        auto_remove=True,  # Whether to automatically remove the container after it completes.
        command='python /app/scripts/create_df.py --data_folder /data',  # The command to run inside the container.
        docker_url='tcp://docker-socket-proxy:2375',  # The URL of the Docker daemon to connect to.
        network_mode='bridge',  # The network mode to use for the container.
        mounts=[
            Mount(source=absolute_data_folder_path, target="/data", type="bind"),
        ],
    )

    # Define the second DockerOperator to run print_df.py
    print_df_op = DockerOperator(
        task_id='print_df',
        image='my_pandas_image',
        api_version='1.30',
        auto_remove=True,
        command='python /app/scripts/print_df.py --df_file_path /data/dataframe.csv',
        docker_url='tcp://docker-socket-proxy:2375',
        network_mode='bridge',
        mounts=[
            Mount(source=absolute_data_folder_path, target="/data", type="bind"),
        ],
    )

    # Set up the task dependencies
    create_df_op >> print_df_op
