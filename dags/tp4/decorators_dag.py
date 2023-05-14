import pendulum

from airflow.decorators import dag, task, task_group

@dag(
    start_date=pendulum.yesterday(),
    schedule="*/1 * * * *",
    catchup=False,
    dag_id="decorator_dag"
)
def my_dag():
    @task
    def get_data(**context):
        return list(range(10))

    @task_group
    def my_task_group(data):
        @task
        def task1(data, **context):
            print(data)
            return data[0]

        @task
        def task2(data, **context):
            print(data)
            return data

        return task2(task1(data))

    my_task_group(get_data())

my_dag()