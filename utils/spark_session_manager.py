
from pyspark.sql import SparkSession

class SparkSessionManager:
    def __init__(self):
        self.spark = None

    def initialize_spark_session(self, app_name):
        if not self.spark:
            self.spark = SparkSession.builder \
                .appName(app_name) \
                .getOrCreate()

    def close_spark_session(self):
        if self.spark:
            self.spark.stop()
            self.spark = None