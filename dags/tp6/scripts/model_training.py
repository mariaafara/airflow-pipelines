import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import boto3
from io import BytesIO
import mlflow.sklearn

if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "--s3_bucket",
        type=str,
        required=True,
        dest="s3_bucket",
        help="S3 bucket name where the data is stored",
    )
    parser.add_argument(
        "--s3_key",
        type=str,
        required=True,
        dest="s3_key",
        help="S3 key (path) to the data file in the bucket",
    )
    parser.add_argument(
        "--exp_name",
        type=str,
        required=True,
        dest="exp_name",
        help="Experiment name.",
    )
    parser.add_argument(
        "--model_name",
        type=str,
        required=True,
        dest="model_name",
        help="Model name.",
    )
    # python scripts/model_training.py --s3_bucket airflow_bucket --s3_key tp6/ml_data/data.csv
    args = parser.parse_args()

    mlflow.set_tracking_uri("http://mlflow-server:5000")
    mlflow.set_experiment(args.exp_name)

    # Initialize a connection to the S3 bucket
    s3 = boto3.client("s3", endpoint_url="http://minio:9000")  # Replace with your Minio server URL
    # Download the data from S3 into a pandas DataFrame
    response = s3.get_object(Bucket=args.s3_bucket, Key=args.s3_key)
    data = pd.read_csv(BytesIO(response["Body"].read()))

    # Assuming your CSV has columns 'x' and 'y', you might do:
    X = data[['x']]
    y = data['y']

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    with mlflow.start_run() as run1:
        params = {"positive": False, "n_jobs": None}

        # Create and train a scikit-learn model (e.g., linear regression)
        model = LinearRegression()
        model.fit(X_train, y_train)

        # Make predictions on the test set
        y_pred = model.predict(X_test)
        # Calculate the Mean Squared Error (MSE) as an example of model evaluation
        mse = mean_squared_error(y_test, y_pred)

        mlflow.log_metric("mse", mse)
        # track model parameters
        mlflow.log_params(model.get_params())
        predictions = model.predict(X_train)

        logged_model = mlflow.sklearn.log_model(model, artifact_path=args.model_name)  # , registered_model_name="LRModel",signature=signature)

        # run_id = mlflow.active_run().info.run_id
        model_uri = logged_model.model_uri
        print(model_uri)
