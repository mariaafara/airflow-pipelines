from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2

default_args = {
    'start_date': datetime(2023, 5, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def extract_data():
    conn = psycopg2.connect(
        host="127.0.0.1",
        port="5432",
        database="postgres",
        user="postgres",
        password="postgres_password"
    )
    cur = conn.cursor()
    cur.execute("SELECT * FROM users")
    data = cur.fetchall()
    print(f"Extracted {len(data)} records.")
    conn.close()
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
    conn = psycopg2.connect(
        host="127.0.0.1",
        port="5432",
        database="postgres",
        user="postgres",
        password="postgres_password"
    )
    cur = conn.cursor()
    for row in transformed_data:
        cur.execute(
            "INSERT INTO transformed_users (id, name_uppercase, email_domain) VALUES (%s, %s, %s)",
            (row[0], row[1], row[2])
        )
    conn.commit()
    conn.close()


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
