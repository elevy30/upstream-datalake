from typing import Callable

from pyspark.sql import DataFrame

from src.car_etl.transformers.car_transformer import Transformer
from src.datalake.datalake import Datalake


class SilverLayer (Datalake):

    def __init__(self, data_path, app_name="SilverApp", bronze_path=None):
        print("Starting Silver Layer")
        super().__init__(app_name=app_name, layer="silver", data_path=data_path)

        self.bronze_path = bronze_path if bronze_path else f"{self.datalake_path}/bronze"


    # Copy data from bronze to silver layer without transformations.
    def bronze_to_silver(self, bronze_entity_name:str, timestamp_col:str=None, transform_func:Callable[[DataFrame], DataFrame]=None):
        print(f"Processing data from bronze layer: {bronze_entity_name}")

        # Read the parquet from bronze
        bronze_entity_path = f"{self.bronze_path}/{bronze_entity_name}"
        df = self.spark_handler.read_parquet(bronze_entity_path)

        df, partition_col = Transformer.add_partition_col(df, timestamp_col, "silver_ingestion_time")

        # Apply transformations if provided
        if transform_func:
            df = transform_func(df)
        else:
            pass

        silver_entity_name = bronze_entity_name.replace("_bronze", "_silver")
        silver_entity_path = f"{self.path}/{silver_entity_name}"
        self.spark_handler.write_parquet(df=df, parquet_path=silver_entity_path, partition_col=partition_col)
        
        print(f"Data copied to silver layer: {silver_entity_path}")
        return df

