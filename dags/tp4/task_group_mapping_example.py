from airflow.decorators import dag, task_group, task
from pendulum import datetime


@dag(
    dag_id="task_group_mapping_example",
    start_date=datetime(2023, 5, 14),
    schedule=None,
    catchup=False,
)
def task_group_mapping_example():
    # creating a task group using the decorator with the dynamic input my_num
    @task_group(group_id="group1")
    def task_group1(my_num):
        @task
        def print_num(num):
            return num

        @task
        def add_25(num):
            return num + 25

        print_num(my_num) >> add_25(my_num)

    # a downstream task to print out resulting XComs
    @task
    def pull_xcom(**context):
        pulled_xcom = context["ti"].xcom_pull(
            # reference a task in a task group with task_group_id.task_id
            task_ids=["group1.add_25"],
            # only pull Xcom from specific mapped task group instances (2.5 feature)
            map_indexes=[2, 3],
            key="return_value",
        )

        # will print out a list of results from map index 2 and 3 of the add_42 task
        print(pulled_xcom)

    # creating 6 mapped task group instances of the task group group1 (2.5 feature)
    task_group1_object = task_group1.expand(my_num=[5, 25, 35, 0, 10])

    # setting dependencies
    task_group1_object >> pull_xcom()


task_group_mapping_example()
