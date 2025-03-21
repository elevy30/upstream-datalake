import os

from src.datalake.spark_handler import SparkHandler


#define as an abstract class
class Datalake:
    datalake_path: str
    path: str
    spark_handler: SparkHandler

    def __init__(self, layer, app_name, data_path, ):
        if not app_name:
            raise ValueError("app_name cannot be empty")
        if not data_path:
            raise ValueError("data_path cannot be empty")

        self.datalake_path = f"{data_path}/datalake"
        self.path = f"{self.datalake_path}/{layer}"
        os.makedirs(self.path, exist_ok=True)

        self.spark_handler = SparkHandler(app_name, f"{data_path}")

    def stop(self):
        self.spark_handler.stop()