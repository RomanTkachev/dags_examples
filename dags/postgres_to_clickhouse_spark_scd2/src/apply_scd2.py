import logging
from functools import reduce
import pendulum
from airflow.hooks.base import BaseHook
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.spark_session import build_spark

CH_TABLE = "users_history"
SCD_END_DATE = "9999-12-31" # заглушка для актуальных периодов
ATTRIBUTE_COLUMNS = ("name", "city", "phone")


def _jdbc_clickhouse_url(conn) -> str:
    database = conn.schema or "default"
    return (
        f"jdbc:clickhouse://{conn.host}:{conn.port or 8123}/{database}"
    )


def _load_history(spark, conn):
    return (
        spark.read.format("jdbc")
        .option("url", _jdbc_clickhouse_url(conn))
        .option("dbtable", CH_TABLE)
        .option("user", conn.login)
        .option("password", conn.password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .load()
        .filter(F.col("is_current") == True) # фильтруем только актуальные записи
    )


def _attribute_changed_condition(): # промежуточная функция, которая проверяет, изменились ли атрибуты
    return reduce(
        lambda left, right: left | right,
        [
            ~F.col(f"s.{column}").eqNullSafe(F.col(f"h.{column}"))
            for column in ATTRIBUTE_COLUMNS
        ],
    )


def apply_scd2(**context):
    logger = logging.getLogger("airflow.task")
    ch_conn = BaseHook.get_connection("conn_clickhouse")

    source_meta = context["ti"].xcom_pull(task_ids="load_from_postgres")
    source_path = source_meta["source_path"]

    ds = context["ds"]
    effective_from = ds
    effective_to_close = (
        pendulum.parse(ds).subtract(days=1).to_date_string()
    )

    spark = build_spark("users_scd2_apply")

    source = spark.read.parquet(source_path).alias("s")
    history = _load_history(spark, ch_conn).alias("h")

    max_key_row = history.agg(F.max("customer_key").alias("max_key")).collect()[0]
    max_key = max_key_row["max_key"] or 0

    joined = source.join(
        history,
        F.col("s.customer_id") == F.col("h.customer_id"),
        "left",
    )

    new_customers = joined.filter(F.col("h.customer_id").isNull())
    changed_customers = joined.filter(
        F.col("h.customer_id").isNotNull() & _attribute_changed_condition()
    )

    close_keys = [
        row["customer_key"]
        for row in changed_customers.select("h.customer_key").distinct().collect()
    ] 

    inserts_source = new_customers.select(
        F.col("s.customer_id").alias("customer_id"),
        F.col("s.name").alias("name"),
        F.col("s.city").alias("city"),
        F.col("s.phone").alias("phone"),
    ).unionByName(
        changed_customers.select(
            F.col("s.customer_id").alias("customer_id"),
            F.col("s.name").alias("name"),
            F.col("s.city").alias("city"),
            F.col("s.phone").alias("phone"),
        )
    )

    insert_count = inserts_source.count()
    if insert_count:
        window = Window.orderBy("customer_id")
        inserts = (
            inserts_source.withColumn(
                "customer_key",
                F.lit(max_key) + F.row_number().over(window),
            )
            .withColumn("effective_from", F.lit(effective_from))
            .withColumn("effective_to", F.lit(SCD_END_DATE))
            .withColumn("is_current", F.lit(True))
        )
        insert_rows = [
            [
                row["customer_key"],
                row["customer_id"],
                row["name"],
                row["city"],
                row["phone"],
                row["effective_from"],
                row["effective_to"],
                row["is_current"],
            ]
            for row in inserts.collect()
        ]
    else:
        insert_rows = []

    spark.stop()

    logger.info(
        "SCD2 за %s: закрыть %s версий, вставить %s новых",
        ds,
        len(close_keys),
        len(insert_rows),
    )

    return {
        "close_keys": close_keys,
        "effective_to": effective_to_close,
        "inserts": insert_rows,
        "insert_count": len(insert_rows),
        "close_count": len(close_keys),
    } # ПЕРЕДЕЛАТЬ ПОТОМ: лучше записывать в S3, а не отдавать в xcom
