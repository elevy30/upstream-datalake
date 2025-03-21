from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

from src.datalake.datalake import Datalake

#  SQL injection patterns
regex_patterns = [
    "SELECT|INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|EXEC", # Basic SQL keywords
    "'.*?'.*?'",                                          # Multiple quotes
    "--.*",                                               # SQL line comments
    "/\\*.*?\\*/",                                        # SQL block comments
    ";.*",                                                # Commands with semicolons
    "UNION.*SELECT"                                       # Union-based injection
]

class IntegrityLayer(Datalake):

    def __init__(self, data_path, app_name="IntegrityApp"):
        print("Starting Integrity Layer")
        super().__init__(app_name=app_name, layer="integrity", data_path=data_path)

    def sql_injection_report(self, bronze_df: DataFrame, column_names: list, output_path: str = None) -> DataFrame:

        # Make sure all columns exist in the DataFrame
        for col in column_names:
            if col not in bronze_df.columns:
                raise ValueError(f"Column '{col}' not found in the DataFrame")

        # Create a union of all regex patterns for efficiency
        combined_pattern = "|".join(f"({pattern})" for pattern in regex_patterns)

        # Create an empty DataFrame for the result
        schema = StructType([
            StructField("violating_column", StringType(), False),
            StructField("original_value", StringType(), True),
            StructField("matched_pattern", StringType(), True),
            StructField("detection_time", TimestampType(), False)
        ])
        violations_df = self.spark_handler.spark.createDataFrame([], schema)

        for col_name in column_names:
            # Only check string columns
            if bronze_df.schema[col_name].dataType == StringType():
                # Process each regex pattern separately
                for pattern in regex_patterns:
                    try:
                        # Apply each pattern individually
                        pattern_violations = bronze_df.withColumn(
                            "matched_pattern",
                            F.regexp_extract(F.col(col_name), pattern, 0)
                        ).filter(
                            # Only keep rows where we found a match (non-empty string)
                            F.length(F.col("matched_pattern")) > 0
                        ).select(
                            # Create the report columns
                            F.lit(col_name).alias("violating_column"),
                            F.col(col_name).alias("original_value"),
                            F.current_timestamp().alias("detection_time"),
                            F.col("matched_pattern")
                        )

                        # Union with existing violations
                        violations_df = violations_df.union(pattern_violations)
                    except Exception as e:
                        print(f"Error processing pattern '{pattern}': {str(e)}")
                        # Continue with other patterns
                        continue

        result_df = violations_df.orderBy("violating_column", "original_value")

        output_path = f"{self.path}/sql_injection_report"
        self.spark_handler.write_parquet(df=result_df, parquet_path=output_path)
        print(f"SQL Injection report saved to: {output_path}")

        # Return the report DataFrame
        if result_df.count() > 0:
            print(f"Found {result_df.count()} potential SQL injection attempts")
        else:
            print("No SQL injection attempts detected")

        return result_df