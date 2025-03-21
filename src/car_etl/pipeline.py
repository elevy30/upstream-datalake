import os

from src.car_etl.transformers.car_transformer import Transformer
from src.datalake.bronze.bronze import BronzeLayer
from src.datalake.bronze.integrity import IntegrityLayer
from src.datalake.gold.gold import GoldLayer
from src.datalake.silver.silver import SilverLayer


class Pipeline:
    def __init__(self):
        data_path = os.path.abspath(os.path.join(os.getcwd(), '..', '..', "data"))
        print(f"Data Path: {data_path}")
        os.environ["DATA_PATH"] = data_path

        self.bronze    = BronzeLayer   (data_path, app_name="BronzeApp")
        self.integrity = IntegrityLayer(data_path, app_name="SQLInjectionDetection")
        self.silver    = SilverLayer   (data_path, app_name="SilverApp", bronze_path=self.bronze.path)
        self.gold      = GoldLayer     (data_path, app_name="GoldApp",   silver_path=self.silver.path)


    # Run the full pipeline from bronze to gold.
    def run_full_pipeline(self, file_path=None, entity_name=None):
        bronze_df = self.bronze.fetch_api_to_bronze(
            entity_name=entity_name,
            api_url="http://localhost:9900/upstream/vehicle_messages",
            params={"amount":10000},
            timestamp_col="timestamp"
        )

        # if file_path:
        #     bronze_df = self.bronze.fetch_csv_to_bronze(file_path=file_path, entity_name=entity_name, timestamp_col="timestamp", mode="overwrite")

        self.integrity.sql_injection_report(bronze_df=bronze_df, column_names=["vin", "manufacturer"])

        self.silver.bronze_to_silver(bronze_entity_name=f"{entity_name}_bronze", timestamp_col="timestamp", transform_func=Transformer.silver_transform)

        self.gold.silver_to_gold(silver_entity_name=f"{entity_name}_silver", transform_func=Transformer.vin_last_state_transform)

        self.gold.silver_to_gold(silver_entity_name=f"{entity_name}_silver", transform_func=Transformer.fastest_vehicles_transform)


    def shutdown(self):
        self.bronze.stop()
        self.silver.stop()
        self.gold.stop()


