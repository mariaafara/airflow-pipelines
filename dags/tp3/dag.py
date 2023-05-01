from datetime import datetime
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator

with DAG(
        'pandas_dag',
        description='A DAG that creates a pandas dataframe and passes it to a BashOperator to print it',
        schedule_interval=None,  # '0 0 * * *',
        start_date=datetime(2023, 5, 1),
        catchup=False
) as dag:
    # Define the first DockerOperator to run create_df.py
    create_df_op = DockerOperator(
        task_id='create_df',
        image='my_pandas_image', # The name of the Docker image to use for the task.
        api_version='1.30', # The version of the Docker API to use.
        auto_remove=True, #  Whether to automatically remove the container after it completes.
        command='python /app/scripts/create_df.py', # The command to run inside the container.
        docker_url='tcp://docker-socket-proxy:2375',  # The URL of the Docker daemon to connect to.
        network_mode='bridge', # The network mode to use for the container.
    )

    # Define the second DockerOperator to run print_df.py
    print_df_op = DockerOperator(
        task_id='print_df',
        image='my_pandas_image',
        api_version='1.30',
        auto_remove=True,
        command='python /app/scripts/create_df.py /app/dataframe.csv',
        docker_url='tcp://docker-socket-proxy:2375',  # Set your docker URL,
        network_mode='bridge',
    )

    # Set up the task dependencies
    create_df_op >> print_df_op
