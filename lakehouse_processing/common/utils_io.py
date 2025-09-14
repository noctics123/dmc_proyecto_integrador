# etl_spark/common/utils_io.py
from pyspark.sql import DataFrame

def read_csv_partitioned(spark, path_glob: str) -> DataFrame:
    return spark.read.option("header", True).csv(path_glob)

def write_parquet(df: DataFrame, path: str, mode="append", partitionBy=None):
    w = df.write.mode(mode)
    if partitionBy:
        w = w.partitionBy(partitionBy)
    w.parquet(path)
