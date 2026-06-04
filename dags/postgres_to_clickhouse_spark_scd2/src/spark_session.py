import os

from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]")) # указываем, какой spark использовать
        .config("spark.sql.session.timeZone", "UTC")
    )

    return builder.getOrCreate() # если сессия есть, то берем существующую
