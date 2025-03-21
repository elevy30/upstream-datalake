from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper

from src.datalake.silver.silver import SilverLayer


def test_silver_layer_initialization(test_data_path):
    silver = SilverLayer("TestApp")
    assert silver.path.endswith("/datalake/silver")


def test_bronze_to_silver(spark, test_data_path):
    # Create test bronze data
    test_data = [(1, "test1"), (2, "test2")]
    bronze_df = spark.createDataFrame(test_data, ["id", "name"])
    bronze_path = f"{test_data_path}/datalake/bronze/test_entity_bronze"
    bronze_df.write.mode("overwrite").parquet(bronze_path)

    def transform_func(df):
        return df.withColumn("name_upper", upper(col("name")))  # Changed this line

    silver = SilverLayer("TestApp")
    result_df = silver.bronze_to_silver(
        "test_entity_bronze",
        transform_func=transform_func
    )

    assert isinstance(result_df, DataFrame)
    assert result_df.count() == 2
    assert 'silver_ingestion_time' in result_df.columns
    assert 'name_upper' in result_df.columns

