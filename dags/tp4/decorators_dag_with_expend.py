import pendulum

from airflow.decorators import dag, task, task_group
from airflow.models.param import Param

# Define the DAG using the @dag decorator
@dag(
    start_date=pendulum.yesterday(),  # Set the starting date to yesterday
    schedule=None,  # Disable scheduling to run the DAG only manually
    catchup=False,  # Don't execute any missed DAG runs
    dag_id="dynamic_mapping_task",  # Set the DAG ID to "dynamic_mapping_task"
    params={
        "data_to_process": []  # Define a "data_to_process" parameter
    }
)
def my_dag():
    # Define a task to retrieve data to process using the @task decorator
    @task
    def get_data(**context):
        # Get the "data_to_process" parameter from the DAG run's context
        return context["params"]["data_to_process"]

    # Define a task group to process the data using the @task_group decorator
    @task_group
    def my_task_group(data):
        # Define a task to multiply each item in the data by 2 using the @task decorator
        @task
        def task1(data, **context):
            return data * 2

        # Define a task to add 2 to each item in the data using the @task decorator
        @task
        def task2(data, **context):
            return data + 2

        # Use the expand method to create multiple instances of task1 for each item in the data
        multiple_data = task1.expand(data=data)
        # Pass the output of task1 to task2 as input and return the result
        return task2.expand(data=multiple_data)

    # Use the output of get_data as input to my_task_group
    my_task_group(get_data())

# Call the my_dag function to create an instance of the DAG
my_dag()
