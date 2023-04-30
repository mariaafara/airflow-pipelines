# TP1

Create a DAG that generates random data and uploads it to an S3 bucket using the PythonOperator. The purpose of this
exercise is to practice using a Python callable to perform a specific task, and to learn how to interact with an
external service such as an S3 bucket using an Airflow hook.

Create another DAG that waits for a file to appear in an S3 bucket using the S3KeySensor, then downloads and prints the
contents of the file using the PythonOperator. The purpose of this exercise is to practice using a sensor to wait for a
specific event to occur before triggering downstream tasks, and to learn how to download files from an S3 bucket using
an Airflow hook.

## dag1:

This dag generates random data and writes it to a file, waits for a random amount of time, and then uploads
the file to an **S3 bucket**. The DAG is scheduled to run every 5 minutes, making this a **simple data generation and
upload
pipeline** that runs **periodically**.

This DAG is named "dag1" and has a single task called "process_data". The DAG is scheduled to run every 5 minutes and
has catchup enabled.

The DAG task "process_data" uses the PythonOperator to execute the Python function named "process_data". The function
takes in a dictionary of context variables that are passed by Airflow at runtime.

- It imports the S3Hook class from the airflow.providers.amazon.aws.hooks.s3 module and creates an instance of it using
  the AWS connection ID "
  minio_conn".
- It then imports the tempfile module and creates a NamedTemporaryFile.
- It generates a random wait time between 0 and 60 seconds and prints a random integer between 100 and 1000.
- It writes the random integer to the temporary file and sets the filename as the key for the S3 object.
- Then it sleeps for the random wait time
- And, then it loads the data from the temporary file to the specified key destination in the S3 bucket using the S3Hook
  instance.

The script also defines a global variable called "BUCKET_NAME" which is set to "airflow_bucket". This variable is used
to specify the name of the S3 bucket where the data will be stored.

## Key terms:

- **S3 bucket**: A container for storing objects, such as files or data, in Amazon S3 (Simple Storage Service). Each
  object in S3 is stored in a bucket, and each bucket has a unique name that must be globally unique across all of AWS.
- **catchup**:  A parameter in Airflow that determines whether to trigger DAG runs for any missed or overdue intervals
  when a DAG is first created or enabled. If catchup is set to True, Airflow will run all the missed DAG runs in
  sequence until it reaches the present time.
- schedule_interval:  A parameter in Airflow that specifies the frequency with which a DAG should be triggered. It takes
  a cron-like string or a timedelta object as input and determines how often the DAG runs.
- **context variable**: A dictionary-like object passed by Airflow to each task instance at runtime, containing
  information about the current execution context, such as the DAG run ID, the execution date, and the task instance ID.
    - **execution date**: A context variable in Airflow that represents the date and time at which a task instance is
      scheduled to run. It is a pendulum object that is passed to each task instance as part of the context dictionary
      and can be used to generate dynamic file paths, database queries, or other runtime values.
- pendulum library: A Python library for working with dates, times, and time zones. It provides a more intuitive and
  human-friendly API than the built-in datetime library and supports a wide range of features, including time zone
  conversions, date arithmetic, and date parsing.
- tempfile module: A Python module that provides a way to create temporary files and directories. It is used to create
  temporary local files when downloading files from an S3 bucket (in this case, using the S3Hook.download_file()
  method). The tempfile module provides a simple way to create and delete temporary files without worrying about naming
  conflicts or file cleanup.

## dag2

This dag waits for a specific S3 object to be available in the S3 bucket, downloads the object to a local
file, and prints its contents to the console. The DAG is scheduled to run every 5 minutes, making this a simple data
retrieval pipeline that runs periodically.

The dag is named "dag2" and has two tasks - "s3_sensor" and "load_data". The DAG is scheduled to run every 5 minutes and
has catchup enabled.

The DAG task "s3_sensor" uses the S3KeySensor operator from the airflow.providers.amazon.aws.sensors.s3 module to wait
for a specific S3 object to be available in the specified S3 bucket. The operator is configured with the bucket name,
key name, AWS connection ID, and poke interval. **The mode is set to "reschedule", which means that the task will
continuously check for the existence of the specified S3 object until it is available.**

The DAG task "load_data" uses the PythonOperator to execute the Python function named "load_data". The function takes in
a single argument - a string called **"key" - that specifies the S3 object key to download and print**. The function
imports
the S3Hook class from the airflow.providers.amazon.aws.hooks.s3 module and creates an instance of it using the AWS
connection ID "minio_conn". It then uses the S3Hook instance to download the S3 object specified by the "key" argument
to a local file at "/tmp". The function then opens the local file and prints its contents to the console.

The script also defines two global variables - "BUCKET_NAME" and "KEY_NAME". "BUCKET_NAME" specifies the name of the S3
bucket where the data is stored, and "KEY_NAME" specifies the path to the S3 object. The path contains placeholders for
the current execution date and time, which are filled in by Airflow at runtime.

----
## Dags Operators used: 

- PythonOperator:
- S3KeySensor: 

## minio_conn


go to admin -> connection -> new connection -> choose aws -> .....
name it minio_conn -> use id and key added in the airflow-server/docker-compose of minio
