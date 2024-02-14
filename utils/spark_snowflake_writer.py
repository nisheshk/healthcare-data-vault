import os


class SnowflakeWriter:
    def __init__(self):
        self.sfURL = os.environ["SF_URL"]
        self.sfUser = os.environ["SF_USERNAME"]
        self.sfPassword = os.environ["SF_PASSWORD"]
        self.sfDatabase = os.environ["SF_DATABASE"]
        self.sfSchema = os.environ["SF_SCHEMA"]
        self.sfWarehouse = os.environ["SF_WAREHOUSE"]

    def write_dataframe_to_snowflake(self, df, table_name, mode="append"):
        # Write DataFrame to Snowflake table
        df.write \
            .format("snowflake") \
            .options(
                sfURL=self.sfURL,
                sfUser=self.sfUser,
                sfPassword=self.sfPassword,
                sfDatabase=self.sfDatabase,
                sfSchema=self.sfSchema,
                sfWarehouse=self.sfWarehouse,
                dbtable=table_name,
                column_mapping="name",
                TIMESTAMP_TYPE_MAPPING="TIMESTAMP_LTZ"
            ) \
            .mode(mode) \
            .save()