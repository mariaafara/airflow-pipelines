import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
import random


def validate_data(**context):
    """Generate a random boolean value to simulate data validation.
    In a real-world scenario, this function could contain more complex logic to validate data
    """
    # data = context['ti'].xcom_pull(key='data')

    random_bool = bool(random.getrandbits(1))

    is_valid = random_bool
    return is_valid


def branch_out(**context):
    """Decide whether to continue with downstream processing or send an email notification
    based on the result of the data validation."""
    return 'downstream_processing' if context['ti'].xcom_pull(
        task_ids="validate_data") else 'send_email'


with DAG(
        dag_id="my_branching_dag",
        start_date=pendulum.datetime(2023, 4, 23),
        schedule_interval=None,
        catchup=False,
) as dag:
    # Define a task to generate data
    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=lambda: [1, 2, 3, 4, 5],
        provide_context=True,
        op_kwargs={},
    )

    # Define a task to validate the data
    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        provide_context=True,
    )

    # Define a branching task to decide whether to continue with downstream processing or send an email notification
    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=branch_out,
        provide_context=True, )

    # Define a task to send an email notification
    send_email = EmailOperator(
        task_id="send_email",
        conn_id="gmail_conn",
        to="afaramaria@gmail.com",
        subject="Data validation failed",
        html_content="Data validation failed. Please check your data.",
    )

    # Define the downstream processing task
    downstream_processing = PythonOperator(
        task_id="downstream_processing",
        python_callable=lambda: print("Data processing complete."),
    )

    # Define the dependencies between tasks
    generate_data >> validate_data >> branch
    branch >> [send_email, downstream_processing]
