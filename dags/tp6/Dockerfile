FROM python:3.10-slim

ENV AWS_ACCESS_KEY_ID="minio_root"
ENV AWS_SECRET_ACCESS_KEY="minio_pass"
ENV MLFLOW_S3_ENDPOINT_URL=""

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY scripts/ scripts/