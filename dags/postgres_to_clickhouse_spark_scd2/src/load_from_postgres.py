import logging
import os
from airflow.hooks.base import BaseHook
from src.spark_session import build_spark

PG_DATABASE = "etl"
PG_TABLE = "users"


def load_from_postgres(**context):
    conn = BaseHook.get_connection("conn_pg")
    logger = logging.getLogger("airflow.task")

    ds = context["ds"]
    work_dir = f"/tmp/users_scd2/{ds}"
    os.makedirs(work_dir, exist_ok=True)
    output_path = f"{work_dir}/source" # лучше записывать в S3, а не на воркер airflow, но пока так

    spark = build_spark("users_scd2_load_postgres")

    jdbc_url = (
        f"jdbc:postgresql://{conn.host}:{conn.port or 5432}/{PG_DATABASE}"
    )

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", PG_TABLE)
        .option("user", conn.login)
        .option("password", conn.password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    row_count = df.count()
    df.write.mode("overwrite").parquet(output_path) # лучше записывать в S3, а не на воркер airflow, но пока так
    spark.stop()

    logger.info("Загружено %s пользователей из PostgreSQL (%s)", row_count, PG_TABLE)

    return {"source_path": output_path, "row_count": row_count}
