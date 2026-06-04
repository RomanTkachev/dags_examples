import logging

import clickhouse_connect
from airflow.hooks.base import BaseHook

CH_TABLE = "users_history"
INSERT_COLUMNS = [
    "customer_key",
    "customer_id",
    "name",
    "city",
    "phone",
    "effective_from",
    "effective_to",
    "is_current",
]


def _chunks(items, size):
    for index in range(0, len(items), size):
        yield items[index : index + size]


def upload_to_clickhouse(**context):
    logger = logging.getLogger("airflow.task")

    scd_result = context["ti"].xcom_pull(task_ids="apply_scd2")
    close_keys = scd_result["close_keys"]
    effective_to = scd_result["effective_to"]
    insert_rows = scd_result["inserts"]

    connection = BaseHook.get_connection("conn_clickhouse")
    client = clickhouse_connect.get_client(
        host=connection.host,
        port=connection.port or 8123,
        username=connection.login,
        password=connection.password,
        database=connection.schema or "default",
    )

    for batch in _chunks(close_keys, 500):
        keys_sql = ", ".join(str(key) for key in batch)
        client.command(
            f"""
            ALTER TABLE {CH_TABLE}
            UPDATE
                effective_to = toDate('{effective_to}'),
                is_current = false
            WHERE customer_key IN ({keys_sql})
            """
        )
        logger.info("Закрыто %s версий (effective_to=%s)", len(batch), effective_to)

    if insert_rows:
        client.insert(CH_TABLE, insert_rows, column_names=INSERT_COLUMNS)
        logger.info("Вставлено %s новых версий в %s", len(insert_rows), CH_TABLE)

    if not close_keys and not insert_rows:
        logger.info("Изменений не обнаружено, запись в ClickHouse не требуется")
