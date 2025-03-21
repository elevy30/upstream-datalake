import pandas as pd
from pyspark.sql import DataFrame

from src.datalake.bronze.bronze import BronzeLayer


def test_bronze_layer_initialization(test_data_path):
    bronze = BronzeLayer("TestApp")
    assert bronze.path.endswith("/datalake/bronze")


def test_fetch_api_to_bronze(spark, test_data_path, mocker):
    # Mock API response
    mock_response = mocker.Mock()
    mock_response.json.return_value = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]

    mocker.patch('requests.get', return_value=mock_response)
    
    bronze = BronzeLayer("TestApp")
    result_df = bronze.fetch_api_to_bronze(
        entity_name="test_entity",
        api_url="http://test.com/api",
        params=None
    )
    
    assert isinstance(result_df, DataFrame)
    assert result_df.count() == 2
    assert 'bronze_ingestion_time' in result_df.columns
    
