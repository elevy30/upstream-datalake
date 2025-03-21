# Define schema for vehicle data
from pyspark.sql.types import *

vin_schema = StructType([
    StructField("vin", StringType(), True),
    StructField("manufacturer", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("model", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timestamp", LongType(), True),
    StructField("velocity", IntegerType(), True),
    StructField("frontLeftDoorState", StringType(), True),
    StructField("wipersState", BooleanType(), True),
    StructField("gearPosition", StringType(), True),
    StructField("driverSeatbeltState", StringType(), True)
    ])

poke_schema = StructType([
    StructField("name", StringType(), True),
    StructField("url", StringType(), True)
    ])