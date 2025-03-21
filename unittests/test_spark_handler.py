from pyspark.sql import DataFrame

from src.datalake.spark_handler import SparkHandler


def test_spark_handler_initialization():
    handler = SparkHandler("TestApp", "/tmp")
    assert handler.spark is not None


def test_read_csv(spark, tmp_path):
    # Create test CSV file using spark
    test_data = [(1, "test1"), (2, "test2")]
    test_df = spark.createDataFrame(test_data, ["id", "name"])
    csv_path = str(tmp_path / "test.csv")
    test_df.write.csv(csv_path, header=True)
    
    handler = SparkHandler("TestApp", "/tmp")
    result_df = handler.read_csv(csv_path)
    
    assert isinstance(result_df, DataFrame)
    assert result_df.count() == 2


def test_read_api(mocker):
    # Mock API response
    mock_response = mocker.Mock()
    mock_response.json.return_value = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]

    mocker.patch('requests.get', return_value=mock_response)
    
    handler = SparkHandler("TestApp", "/tmp")
    result_df = handler.read_api("http://test.com/api")
    
    assert isinstance(result_df, DataFrame)
    assert result_df.count() == 2
