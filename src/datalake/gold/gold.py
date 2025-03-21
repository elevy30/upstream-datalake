import os
from typing import Callable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.datalake.datalake import Datalake


class GoldLayer(Datalake):

    def __init__(self, data_path, app_name="GoldApp", silver_path=None):
        print("Starting Gold Layer")
        super().__init__(app_name=app_name, layer="gold", data_path=data_path)

        self.silver_path = silver_path if silver_path else f"{self.datalake_path}/silver"


    def silver_to_gold(self, silver_entity_name:str, transform_func:Callable[[DataFrame], DataFrame]=None):
        print(f"Processing data from silver layer: {silver_entity_name}")

        silver_entity_path = f"{self.silver_path}/{silver_entity_name}"
        df = self.spark_handler.read_parquet(silver_entity_path)

        if transform_func:
            df = transform_func(df)
        else:
            pass

        df = df.withColumn("gold_ingestion_time", F.current_timestamp())

        transform_name = transform_func.__name__.replace("_transform", "")
        gold_entity_name = silver_entity_name.replace("_silver", f"_{transform_name}_gold")
        gold_entity_path = f"{self.path}/{gold_entity_name}"
        self.spark_handler.write_parquet(df=df, parquet_path=gold_entity_path)

        print(f"Data processed to gold layer: {gold_entity_path}")
        return df



