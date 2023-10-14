FROM python:3.11-slim

ENV AWS_ACCESS_KEY_ID="minio_root"
ENV AWS_SECRET_ACCESS_KEY="minio_pass"

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY scripts/ scripts/