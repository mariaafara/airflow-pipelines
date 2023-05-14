from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.decorators import dag, task, task_group


# Define the DAG
@dag(
    start_date=datetime(2023, 5, 7),
    catchup=False,
    dag_id="multiple_task_group_dag",
    schedule_interval=None,  # Set to None for manual triggering
    description='A DAG with multiple task groups',

)
def my_dag():
    # Define the number of repetitions for the task group
    n = 3

    # Define the first task
    task1 = DummyOperator(
        task_id='task1'
    )

    # Define the task group
    @task_group(group_id="task_group")
    def create_task_group():
        tasks = []
        # Repeat the task group n times
        for i in range(n):
            # Define the tasks within the task group
            task2 = DummyOperator(
                task_id=f'task2_{i}'
            )
            task3 = DummyOperator(
                task_id=f'task3_{i}'
            )
            # Set the task dependencies within the task group
            tasks.append(task1 >> task2 >> task3)

        return tasks

    create_task_group()


my_dag()
