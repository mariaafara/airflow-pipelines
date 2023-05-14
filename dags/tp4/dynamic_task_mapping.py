from airflow.decorators import dag, task, task_group

from datetime import datetime


# Define the DAG
@dag(
    'dynamic_task_mapping_example1',
    start_date=datetime(2023, 5, 10),
    schedule_interval=None
)
def my_dag():
    @task
    def add(x: int, y: int):
        return x + y

    # This expand function creates three mapped add tasks, one for each entry in the x input list.
    # The partial function specifies a value for y that remains constant in each task.
    added_values = add.partial(y=10).expand(x=[1, 2, 3])


# Call the my_dag function to create an instance of the DAG
my_dag()
