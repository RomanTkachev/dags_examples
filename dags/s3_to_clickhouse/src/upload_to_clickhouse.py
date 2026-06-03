from airflow.hooks.base import BaseHook


def upload_to_clickhouse(**context):
    import logging
    import pendulum
    import clickhouse_connect

    logger = logging.getLogger("airflow.task")

    stats = context["ti"].xcom_pull(task_ids="load_from_s3")
    week = stats["week"]
    visits = stats["visits"]
    loaded_at = pendulum.now("UTC").naive()

    connection = BaseHook.get_connection("conn_clickhouse")

    client = clickhouse_connect.get_client(
        host=connection.host,
        port=connection.port or 8123,
        username=connection.login,
        password=connection.password,
        database=connection.schema or "default",
    )

    client.command(
        f"ALTER TABLE weekly_visits_stats DELETE WHERE week = toDate('{week}')"
    )
    client.insert(
        "weekly_visits_stats",
        [[week, visits, loaded_at]],
        column_names=["week", "visits", "loaded_at"],
    )

    logger.info(f"Записана статистика за неделю {week}: {visits} визитов")
