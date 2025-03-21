import os
import pytest
from pyspark.sql import SparkSession

# Global variable to hold the shared Spark session
_spark = None

@pytest.fixture(scope="session")
def spark():
    global _spark
    
    if _spark is None:
        # Create new Spark session only if it doesn't exist
        _spark = (SparkSession.builder
                 .appName("UnitTest")
                 .master("local[*]")
                 .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                 # Add these configs to ensure proper local execution
                 .config("spark.driver.host", "localhost")
                 .config("spark.driver.bindAddress", "localhost")
                 .config("spark.executor.memory", "1g")
                 .config("spark.driver.memory", "1g")
                 .getOrCreate())

    yield _spark

    # Only stop Spark at the very end of all tests
    if _spark is not None:
        _spark.stop()
        _spark = None


@pytest.fixture(scope="session")
def test_data_path(tmp_path_factory):
    return str(tmp_path_factory.mktemp("data"))


@pytest.fixture(autouse=True)
def setup_env(test_data_path):
    os.environ["DATA_PATH"] = test_data_path