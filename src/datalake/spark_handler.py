import json
from typing import List

import requests
from pyspark.sql import SparkSession


class SparkHandler:
    spark = None

    def __init__(self, app_name, base_path, log_level="WARN"):
        # findspark.init()

        # spark_temp_dir = os.path.join(base_path, "spark/spark-temp")
        # os.makedirs(spark_temp_dir, exist_ok=True)
        #
        # # check file path exist
        # warehouse_path = os.path.join(base_path, "spark/spark-warehouse")
        # os.makedirs(warehouse_path, exist_ok=True)

        self.spark = (SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            # .config("spark.local.dir", spark_temp_dir)
            # .config("spark.sql.warehouse.dir", warehouse_path)
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.cleaner.referenceTracking.cleanCheckpoints", "false")
            .getOrCreate())

        self.spark.sparkContext.setLogLevel(log_level)

    def stop(self):
        self.spark.stop()

    def read_csv(self, path):
        options = {"header": "true", "inferSchema": "true"}
        df = self.spark.read.csv(path, **options)
        return df

    def read_parquet(self, parquet_path):
        df = self.spark.read.format("parquet").load(parquet_path)
        return df

    def read_api(self, api_url, params=None, schema=None):
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()

        json_rdd = self.spark.sparkContext.parallelize([json.dumps(record) for record in data])
        if schema:
            df = self.spark.read.json(json_rdd, schema=schema)
        else:
            df = self.spark.read.json(json_rdd)

        return df

    @staticmethod
    def write_parquet(df, parquet_path, partition_col: List[str] = None, mode="overwrite"):
        if partition_col:
            df.write.format("parquet").mode(mode).partitionBy(*partition_col).save(parquet_path)
        else:
            df.write.format("parquet").mode(mode).save(parquet_path)
