import os

from src.car_etl.pipeline import Pipeline


class Main:
    # Example usage:
    if __name__ == "__main__":
        pipeline = Pipeline()

        # sample_data = f"{os.getenv('DATA_PATH')}/sample_car_data/car_data_batch_1_20250321125244.csv"
        pipeline.run_full_pipeline(entity_name="car")
        pipeline.shutdown()
