import os

from src.car_etl.transformers.car_transformer import Transformer
from src.datalake.datalake import Datalake


class BronzeLayer (Datalake):

    def __init__(self, data_path, app_name="BronzeApp"):
        print("Starting Bronze Layer")
        super().__init__(app_name=app_name, layer="bronze", data_path=data_path)


    def fetch_csv_to_bronze(self, file_path, entity_name:str=None, timestamp_col:str=None, mode="overwrite"):
        print(f"Data ingested to bronze layer: {file_path}")
        df = self.spark_handler.read_csv(file_path)

        source_name = (os.path.basename(file_path).split('.')[0]
                       if '.' in os.path.basename(file_path)
                       else os.path.basename(file_path))
        entity_name = entity_name if entity_name else f"{source_name}"

        return self.write_parquet(df=df, entity_name=entity_name, timestamp_col=timestamp_col, mode=mode)


    def fetch_api_to_bronze(self, entity_name:str, api_url:str, params:dict, schema=None, timestamp_col:str=None, mode="overwrite"):
        print(f"Ingesting data from an API: {api_url}")
        df = self.spark_handler.read_api(api_url, params, schema)

        return self.write_parquet(df=df, entity_name=entity_name, timestamp_col=timestamp_col, mode=mode)


    def write_parquet(self, df, entity_name, timestamp_col, mode):
        df, partition = Transformer.add_partition_col(df, timestamp_col, "bronze_ingestion_time")

        bronze_entity_path = f"{self.path}/{entity_name}_bronze"
        self.spark_handler.write_parquet(df=df, parquet_path=bronze_entity_path, partition_col=partition, mode=mode)

        print(f"Data ingested to bronze layer: {bronze_entity_path}")
        print(f"Records ingested: {df.count()}")
        return df



