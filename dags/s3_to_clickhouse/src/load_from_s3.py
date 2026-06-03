from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_BUCKET = "dev"
S3_PREFIX = "not_so_bad/metrika/traffic_data_"


def load_from_s3(**context):
    import logging
    import pandas as pd
    from io import BytesIO
    import pendulum

    logger = logging.getLogger("airflow.task")

    execution_date = pendulum.parse(context["ds"])
    week_end = execution_date.subtract(days=1)
    week_start = week_end.subtract(days=6)

    hook = S3Hook(aws_conn_id="minios3_conn")
    total_visits = 0
    days_loaded = []

    current = week_start
    while current <= week_end:
        ds = current.to_date_string()
        key = f"{S3_PREFIX}{ds}.parquet"

        if hook.check_for_key(key, bucket_name=S3_BUCKET):
            obj = hook.get_key(key, bucket_name=S3_BUCKET)
            df = pd.read_parquet(BytesIO(obj.get()["Body"].read()))
            total_visits += int(df["visits"].sum())
            days_loaded.append(ds)
            logger.info(f"Прочитан файл {key}, visits={df['visits'].sum()}")
        else:
            logger.warning(f"Файл {key} не найден в bucket {S3_BUCKET}")

        current = current.add(days=1)

    logger.info(
        f"Неделя {week_start.to_date_string()} — {week_end.to_date_string()}: "
        f"загружено {len(days_loaded)} дней, сумма visits={total_visits}"
    )

    return {
        "week": week_start.to_date_string(),
        "visits": total_visits,
        "days_loaded": days_loaded,
    }
