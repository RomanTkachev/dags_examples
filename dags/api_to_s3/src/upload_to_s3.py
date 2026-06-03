from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload_to_s3(**context):
    import logging
    import pandas as pd
    from io import BytesIO

    logger = logging.getLogger('airflow.task')

    df_dict = context["ti"].xcom_pull(task_ids="load_from_api")
    df = pd.DataFrame(df_dict)

    filename = f"not_so_bad/metrika/traffic_data_{context['ds']}.parquet"
    
    if not df.empty:
        file = BytesIO()
        df.to_parquet(file, index=False)
        file.seek(0)

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=file.read(),
            key=filename,
            bucket_name="dev",
            replace=True,
        )

        logger.info(f"Сохранён датафрейм длиной {len(df)}")