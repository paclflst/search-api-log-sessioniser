from xmlrpc.client import Boolean
from modules.data_reader import DataReader
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from os.path import isdir

class StorageService(DataReader):
    def __init__(self, spark: SparkSession):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        self.key_cols = ['entity_id']

    def get_df_from_file(self, spark: SparkSession, schema: StructType, key_cols: list, path: str, base_path: str=None) -> DataFrame :
        if base_path is None:
            base_path = path
        df = spark \
            .read \
            .option('mode', 'DROPMALFORMED') \
            .option('basePath', base_path) \
            .json(f'{path}/*.json', schema=schema, allowBackslashEscapingAnyCharacter=True)

        df = df.repartition(*key_cols)
        return df

    def save_df_to_file(self, df: DataFrame, path: str, key_cols: list, mode: str = 'append'):
        df.write \
            .format('json') \
            .partitionBy(*key_cols)\
            .mode(mode) \
            .save(path)
        return True

    def path_exists(self, path: str) -> Boolean:
        return isdir(path)