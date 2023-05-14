import json
from airflow.decorators import dag, task, task_group

import pendulum


@dag(schedule=None, start_date=pendulum.datetime(2023, 5, 14, tz="UTC"), catchup=False)
def task_group_example1():
    @task(task_id="extract", retries=2)
    def extract_data():
        data_string = '{"1": 100, "2": 50, "3": 200, "4": 600}'
        data_dict = json.loads(data_string)
        return data_dict

    @task_group
    def transform_values(data_dict):

        @task()
        def transform_sum(data_dict: dict):
            total_value = 0
            for value in data_dict.values():
                total_value += value

            return {"total_value": total_value}

        @task()
        def transform_avg(data_dict: dict):
            total_value = 0
            avg_value = 0
            for value in data_dict.values():
                total_value += value
                avg_value = total_value / len(data_dict)

            return {"avg_value": avg_value}

        return {
            "avg": transform_avg(data_dict),
            "total": transform_sum(data_dict),
        }

    @task()
    def load(values: dict):
        print(
            f"""Total value is: {values['total']['total_value']:.2f} 
            and average value is: {values['avg']['avg_value']:.2f}"""
        )

    load(transform_values(extract_data()))  # eq to data >> transfer_values >> load


task_group_example1 = task_group_example1()  # the name of the dag function is the dag id
