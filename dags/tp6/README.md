## Task 1: Generate and Upload data

## minio_conn

Within the Airflow UI, go to Admin -> Connections

Create a new connection with the name minio_conn.

Enter minio_root for the Access Key and minio_pass fpr the Secret Key.

In Extras, let's set the URL to our local MinIO deployment with the following syntax

<ip> = minio because it is the name of the minio service connected to airflow.

`{ "endpoint_url": "http://minio:9000" }`

go to admin -> connection -> new connection -> choose aws -> .....

name it minio_conn -> use id and key added in the airflow-server/docker-compose of minio

-------

## Docker setup for the train_model task

```bash
cd dags/tp6
docker build -t ml_scripts_image .
```

docker run --name ml_scripts_container ml_scripts_image

docker run -d --name ml_scripts_container ml_scripts_image
docker start ml_scripts_container

docker exec -it ml_scripts_container /bin/bash

----
Tech Stack
MLflow: For experiment tracking and model registration
PostgreSQL: Store the MLflow tracking
Amazon S3: Store the registered MLflow models and artifacts
Apache Airflow: Orchestrate the MLOps pipeline
Scikit-learn: Machine Learning

----
## Dag tasks:

register and stage: registers a model with MLflow, checks if there is a current production model, compares the new model
with the current production model, and promotes it to the "Staging" stage if it's better.


----
# Deploying with fastapi in a containarized env

```bash
cd dags/tp6/
cd app/
docker build -t lr_app .
```
Then run it with:

```bash
docker run -p 8090:8090 -e MLFLOW_TRACKING_URI='http://mlflow-server:5000' --network "docker-compose_default" lr_app 
```


Then access: http://0.0.0.0:8090/docs

[//]: # (`docker run -it --rm lr_app bash`)
