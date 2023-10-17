import mlflow
import mlflow.sklearn
from mlflow import MlflowClient

if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "--model_name",
        type=str,
        required=True,
        dest="model_name",
        help="model name to be registered",
    )
    parser.add_argument(
        "--model_uri",
        type=str,
        required=True,
        dest="model_uri",
        help="The uri of the model logged after an experiment run.",
    )
    # python scripts/model_training.py --s3_bucket airflow_bucket --s3_key tp6/ml_data/data.csv
    args = parser.parse_args()

    mlflow.set_tracking_uri("http://mlflow-server:5000")

    print("Model URI:", args.model_uri)

    registered_model_name = args.model_name

    # register the model
    register_model_result = mlflow.register_model(model_uri=args.model_uri,
                                                  name=registered_model_name)  # create a new registered model if not exist with v1 else a new version for the already existed model

    client = MlflowClient(tracking_uri="http://mlflow-server:5000")

    # 2. Get the production model (if it exists)
    try:
        current_production_version = client.get_latest_versions(name=registered_model_name, stages=["Production"])[0]
        print("Got the current production model,", current_production_version)
    except IndexError:
        print("No production model yet.")
        current_production_version = None

    # 3. Compare the new model with the current production model (if it exists)
    if current_production_version:
        prod_model_run = mlflow.search_runs(filter_string=f"attributes.run_id = '{register_model_result.run_id}'",
                                            output_format="list")
        current_model_run = mlflow.search_runs(
            filter_string=f"attributes.run_id = '{current_production_version.run_id}'", output_format="list")
        prod_model_metric = prod_model_run[0].data.metrics
        current_model_metric = current_model_run[0].data.metrics
        if current_model_metric["mse"] > prod_model_metric["mse"]:
            # 4. If the new model is better, promote it to "Staging"
            client.transition_model_version_stage(
                name=registered_model_name, version=register_model_result.version, stage="Staging"
            )
            print("Model promotion: New model is better and is now in 'Staging' stage.")
        else:
            print("Model promotion: New model did not meet criteria to be in 'Staging' stage.")
    else:
        client.transition_model_version_stage(
            name=registered_model_name, version=register_model_result.version, stage="Staging"
        )
        print("Version 1 of the New model is now in 'Staging' stage since no model is yet in 'Production'")
