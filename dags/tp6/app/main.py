from fastapi import FastAPI
import mlflow.pyfunc
# from mlflow import MlflowClient
import os

app = FastAPI()

os.environ["AWS_ACCESS_KEY_ID"] = "minio_root"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minio_pass"

# Define a class to manage the model
class ModelManager:
    def __init__(self):
        # Load the model during initialization
        model_name = "lr_model"
        stage = 'Staging'

        self.model = mlflow.pyfunc.load_model(
            model_uri=f"models:/{model_name}/{stage}"
        )


# Create an instance of the ModelManager
model_manager = ModelManager()


@app.post("/predict")
def predict(input_data: int):
    # Use the loaded model for predictions
    prediction = model_manager.model.predict([[input_data]])
    print(prediction)
    print("..")
    return {"prediction": prediction[0]}
