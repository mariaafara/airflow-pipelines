import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import boto3
from io import BytesIO

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
    # python scripts/model_training.py --s3_bucket airflow_bucket --s3_key tp6/ml_data/data.csv
    args = parser.parse_args()

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

    # Create and train a scikit-learn model (e.g., linear regression)
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Make predictions on the test set
    y_pred = model.predict(X_test)

    # Calculate the Mean Squared Error (MSE) as an example of model evaluation
    mse = mean_squared_error(y_test, y_pred)

    # You can save the trained model, plot results, or perform any other actions here
    # For demonstration, we'll just print the MSE
    print(f"Mean Squared Error: {mse}")
