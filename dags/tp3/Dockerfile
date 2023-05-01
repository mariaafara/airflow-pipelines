FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
COPY scripts/ ./scripts/
RUN pip install -r requirements.txt #--no-cache-dir

