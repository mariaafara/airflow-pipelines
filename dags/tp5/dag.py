from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'start_date': datetime(2023, 5, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def extract_data():
    hook = PostgresHook(postgres_conn_id="my_postgres_connection")
    data = hook.get_records("SELECT * FROM users")
    print(f"Extracted {len(data)} records.")
    return data


def transform_data(data):
    transformed_data = []
    for row in data:
        # Add your data transformation logic here
        transformed_row = [row[0], row[1].upper(), row[2].split('@')[1]]
        print(transformed_row)
        transformed_data.append(transformed_row)
    return transformed_data


def store_data(transformed_data):
    hook = PostgresHook(postgres_conn_id="my_postgres_connection")
    for row in transformed_data:
        hook.insert_records("INSERT INTO transformed_users (id, name_uppercase, email_domain) VALUES (%s, %s, %s)",
                            (row[0], row[1], row[2]))


with DAG('postgress_dag', default_args=default_args, schedule_interval=None) as dag:
    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={'data': extract.output}
    )

    store = PythonOperator(
        task_id='store_data',
        python_callable=store_data,
        op_kwargs={'transformed_data': transform.output}
    )

    extract >> transform >> store
