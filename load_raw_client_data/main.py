"""
spark-submit --master 'local[*]' \
             --packages net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4 \
             --jars ../gcs-connector-latest-hadoop2.jar \
             --conf "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" \
             load_raw_client_data/main.py  \
             batch_id=68b75890-b108-4b95-8330-876c9c5c5884 file_name=gs://client3_inut_file/client3.csv client_name=test
"""

import sys
sys.path.append("..")

import re
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import current_timestamp, lit, struct, to_json, current_timestamp
from healthcare_data_vault.utils.spark_snowflake_writer import SnowflakeWriter
from healthcare_data_vault.utils.spark_session_manager import SparkSessionManager

class RawDataLoader(SparkSessionManager, SnowflakeWriter):
    def __init__(self, args_dict, app_name):
        SparkSessionManager.__init__(self)
        SnowflakeWriter.__init__(self)
        self.args_dict = args_dict
        self.initialize_spark_session(app_name)

    def load_client_file_metadata(self):
        metadata_schema = StructType([
            StructField("BATCH_ID", StringType(), nullable=False),
            StructField("FILE_NAME", StringType(), nullable=True),
            StructField("CLIENT_NAME", StringType(), nullable=True),
            StructField("PROCESSING_STATUS", IntegerType(), nullable=False),
        ])
        file_name = self.args_dict.get("file_name")
        batch_id = self.args_dict.get("batch_id")
        client_name = self.args_dict.get("client_name")
        metadata_df = self.spark.createDataFrame([(batch_id, file_name, client_name, 0)], metadata_schema)
        metadata_df = metadata_df.withColumn('LOAD_TIMESTAMP', current_timestamp().cast(TimestampType()))
        super().write_dataframe_to_snowflake(df=metadata_df, table_name="RAW_CLIENT_INPUT_METADATA", mode="append")

    def load_client_file_data(self):
        batch_id = self.args_dict.get("batch_id")
        self.spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile","../service_account.json")
        raw_data_df = self.spark.read.csv(self.args_dict.get("file_name"), header=True)
        raw_data_df = raw_data_df.select(to_json(struct(raw_data_df.columns)).alias("RAW_DATA"))
        raw_data_df = raw_data_df.withColumn("BATCH_ID", lit(batch_id)).withColumn("LOAD_TIMESTAMP", current_timestamp().cast(TimestampType()))
        super().write_dataframe_to_snowflake(raw_data_df, 'RAW_CLIENT_INPUT_DATA')

if __name__ == '__main__':
    args_dict = {arg.split('=')[0]: arg.split('=')[1] for arg in sys.argv[1:]}
    print(args_dict)
    if "batch_id" not in args_dict or "file_name" not in args_dict or "client_name" not in args_dict:
        raise ValueError("Missing either batch_id, file_name or client_name argument")


data_loader = RawDataLoader(args_dict, app_name="Load raw client data")
data_loader.load_client_file_metadata()
data_loader.load_client_file_data()
data_loader.close_spark_session()