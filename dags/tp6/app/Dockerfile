# Base image
FROM python:3.10-slim
# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY requirements.txt /app

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

ENV PORT=8090

# Define environment variables
ENV MLFLOW_TRACKING_URI='http://mlflow-server:5000'
ENV MLFLOW_S3_ENDPOINT_URL='http://minio:9000'

# Make port 8080 available to the world outside this container
EXPOSE 8090

COPY main.py /app

# Run FastAPI when the container launches
CMD uvicorn main:app --host 0.0.0.0 --port $PORT --reload --workers 1
