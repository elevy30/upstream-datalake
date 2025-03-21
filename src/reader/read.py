import os

from src.datalake.bronze.bronze import BronzeLayer
from src.datalake.bronze.integrity import IntegrityLayer
from src.datalake.gold.gold import GoldLayer
from src.datalake.silver.silver import SilverLayer


class Main:
    def __init__(self, base_table_name=None):
        data_path = os.path.abspath(os.path.join(os.getcwd(), '..', '..', "data"))
        print(f"Data Path: {data_path}")
        os.environ["DATA_PATH"] = data_path
        self.bronze = BronzeLayer(data_path, app_name="BronzeApp")
        self.integrity = IntegrityLayer(data_path, app_name="IntegrityApp")
        self.silver = SilverLayer(data_path, app_name="SilverApp", bronze_path=self.bronze.path)
        self.gold   = GoldLayer(data_path, app_name="GoldApp",   silver_path=self.silver.path)

    def print_all_layers(self):
        print("Bronze Layer: ------------ car_bronze ------------------")
        self.bronze.spark_handler.read_parquet(f"{self.bronze.path}/car_bronze").show()
        print("Integrity:")
        self.integrity.spark_handler.read_parquet(f"{self.integrity.path}/sql_injection_report").show()
        print("Silver Layer: ------------ car_silver ------------------")
        self.silver.spark_handler.read_parquet(f"{self.silver.path}/car_silver").show()
        print("Gold Layer: ------------ car_vin_last_state_gold ------------------")
        self.gold.spark_handler.read_parquet(f"{self.gold.path}/car_vin_last_state_gold").show()
        print("Gold Layer: ------------ car_fastest_vehicles_gold ------------------")
        self.gold.spark_handler.read_parquet(f"{self.gold.path}/car_fastest_vehicles_gold").show()

    def shutdown(self):
        self.bronze.stop()
        self.silver.stop()
        self.gold.stop()

# Example usage:
if __name__ == "__main__":
    main = Main(base_table_name="car")

    try:
        main.print_all_layers()
    except Exception as e:
        print(e)

    main.shutdown()