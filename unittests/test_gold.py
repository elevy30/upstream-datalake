from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length

from src.datalake.gold.gold import GoldLayer


def test_gold_layer_initialization(test_data_path):
    gold = GoldLayer("TestApp")
    assert gold.path.endswith("/datalake/gold")


def test_silver_to_gold(spark, test_data_path):
    # Create test silver data
    test_data = [(1, "test1"), (2, "test2")]
    silver_df = spark.createDataFrame(test_data, ["id", "name"])
    silver_path = f"{test_data_path}/datalake/silver/test_entity_silver"
    silver_df.write.mode("overwrite").parquet(silver_path)
    
    def transform_func(df):
        return df.withColumn("name_length", length(col("name")))
    
    gold = GoldLayer("TestApp")
    result_df = gold.silver_to_gold(
        "test_entity_silver",
        transform_func=transform_func
    )
    
    assert isinstance(result_df, DataFrame)
    assert result_df.count() == 2
    assert 'gold_ingestion_time' in result_df.columns
    assert 'name_length' in result_df.columns
    
