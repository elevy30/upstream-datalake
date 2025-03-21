from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

class Transformer:

    @staticmethod
    def add_partition_col(df, timestamp_col, ingestion_time_col):
        df = df.withColumn(ingestion_time_col, F.current_timestamp())
        if timestamp_col:
            df = df.withColumn("date", F.date_format(F.from_unixtime(F.col(timestamp_col) / 1000), "yyyy-MM-dd"))
            df = df.withColumn("hour", F.date_format(F.from_unixtime(F.col(timestamp_col) / 1000), "HH"))
        else:
            df = df.withColumn("date", F.date_format(F.col(ingestion_time_col), "yyyy-MM-dd"))
            df = df.withColumn("hour", F.date_format(F.col(ingestion_time_col), "HH"))

        partition_col = ["date", "hour"]
        return df, partition_col

    @classmethod
    def silver_transform(cls, df):
        df = cls.timestamp_transform(df)
        df = cls.cleanup_transform(df)
        df = cls.gear_2_num_transform(df)
        return df

    @staticmethod
    def timestamp_transform(df):
        transformed_df = df.withColumn("silver_processing_time", F.current_timestamp())
        return transformed_df

    @staticmethod
    def cleanup_transform(df):
        transformed_df = df.withColumn("manufacturer", F.trim(F.col("manufacturer")))
        transformed_df = transformed_df.filter(transformed_df.vin.isNotNull())
        return transformed_df

    @staticmethod
    def gear_2_num_transform(df):
        def standardize_gear(gear_col_val):
            return (
                F.when(gear_col_val.isin(["P", "PARK"]), "0")
                .when(gear_col_val.isin(["R", "REVERSE"]), "-1")
                .when(gear_col_val.isin(["N", "NEUTRAL"]), "0")
                .when(gear_col_val.isin(["D", "DRIVE"]), "1")
                .when(gear_col_val.rlike("^[0-9]+$"), gear_col_val)  # Already numeric
                .otherwise("0")  # Default to 1 for any other values
            ).cast(IntegerType())

        transformed_df = df.withColumn("gearPosition", standardize_gear(F.col("gearPosition")))
        return transformed_df

    @staticmethod
    def vin_last_state_transform(df):
        vin_last_state_report = (
             df.groupBy("vin").agg(
                             F.max("timestamp").alias("last_reported_timestamp"),
                             F.last("frontLeftDoorState", ignorenulls=True).alias("front_left_door_state"),
                             F.last("wipersState", ignorenulls=True).alias("wipers_state"))
            )
        return vin_last_state_report

    @staticmethod
    def fastest_vehicles_transform(df):
        # Find the maximum velocity for each vehicle in each hour
        max_velocity_per_vin_hour = df.groupBy("vin", "hour").agg(F.max("velocity").alias("max_velocity"))

        hour_window = Window.partitionBy("hour").orderBy(F.desc("max_velocity"))
        # get the top 10
        top_10_df = (max_velocity_per_vin_hour.withColumn("rank", F.row_number().over(hour_window))
                                              .filter(F.col("rank") <= 10))

        fastest_vehicles_report = (top_10_df.select("vin","hour","max_velocity")
                                            .orderBy("hour", F.desc("max_velocity")))

        return fastest_vehicles_report



    # @staticmethod
    # def vin_last_state_transform(df):
    #
    #     # Get the most recent timestamp for each VIN
    #     vin_window = Window.partitionBy("vin").orderBy(F.desc("timestamp"))
    #     latest_timestamp_df = (df.withColumn("row_number", F.row_number().over(vin_window))
    #                              .filter(F.col("row_number") == 1)
    #                              .select("vin", F.col("timestamp").alias("last_reported_timestamp")))
    #
    #     # Find the last non-null door state for each VIN
    #     door_window = Window.partitionBy("vin").orderBy(F.desc("timestamp"))
    #     door_state_df = df.filter(F.col("frontLeftDoorState").isNotNull())
    #     door_state_df = (door_state_df.withColumn("row_number", F.row_number().over(door_window))
    #                                   .filter(F.col("row_number") == 1)
    #                                   .select("vin", F.col("frontLeftDoorState").alias("front_left_door_state")))
    #
    #     # Find the last non-null wipers state for each VIN
    #     wipers_window = Window.partitionBy("vin").orderBy(F.desc("timestamp"))
    #     wipers_state_df = df.filter(F.col("wipersState").isNotNull())
    #     wipers_state_df = (wipers_state_df.withColumn( "row_number", F.row_number().over(wipers_window))
    #                                       .filter(F.col("row_number") == 1)
    #                                       .select("vin", F.col("wipersState").alias("wipers_state")))
    #
    #     # Join all the information together
    #     vin_last_state_report = (latest_timestamp_df
    #                                 .join(door_state_df, "vin", "left")
    #                                 .join(wipers_state_df, "vin", "left")
    #     )
    #     return vin_last_state_report