##  Алгоритм такой:
# 1. Получаем данные из API. Сохраняем в датафрейм. 
# 2. Грузим датафрейм во временную таблицу temp_leads в postgre
# 3. Делаем merge временной таблицы temp_leads в основную таблицу leads по id заявки.
#    Новые заявки добавятся, заявки с апдейтами – обновятся, заявки, которых нет в источнике – удалятся.
# 4. Сносим временную таблицу temp_leads после успешного коммита.

from airflow.hooks.base import BaseHook

def load_to_postgre (**context):
    import requests 
    import pandas as pd
    import json
    from requests.exceptions import Timeout
    import logging
    import numpy as np
    import psycopg2 as pg
    from psycopg2.extras import execute_values

    URL = "https://run.mob-edu.ru/webhook/da-test-sample"
    PG_DATABASE = "lanit_test"

    logger = logging.getLogger('airflow.task')
    logger.info(f"Попытка подключения к API url={URL}")

    try:
        response = requests.get(URL, timeout=20)
        logger.debug(f"Получен ответ со статусом {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            logger.info(f"Успешное подключение")
        else:
            logger.warning(
                    f"API вернул ошибку {response.status_code}"
                    f"Ответ: {response.text[:200]}"
                )
            response.raise_for_status() 

    except Timeout:
            logger.error(
                f"Таймаут подключения к API (превышено 20 секунд)",
                exc_info=True
            )
            raise

    logger.debug(f"Формируем датафрейм с данными")

    df = pd.json_normalize(data) #делаем плоскую таблицу

    if df.empty:
        logger.error(f"Датафрейм пустой. Merge выполнять нельзя: очистится продовая таблица", exc_info=True)
        raise

    logger.debug(f"Меняем типы данных")
    df = df.replace({np.nan: None})
    df["PHONE"] = df["PHONE"].apply(lambda x: json.dumps(x))
    df["EMAIL"] = df["EMAIL"].apply(lambda x: json.dumps(x))
    df["ID"] = df['ID'].astype(int)
    

    logger.debug(f"Меняем названия колонок")
    df.rename(columns={
        "ID": "lead_id",
        "TITLE": "title",
        "NAME": "first_name",
        "LAST_NAME": "last_name",
        "STATUS_ID":"status_id",
        "SOURCE_ID":"source_id",
        "UF_CLIENT_TYPE":"client_type",
        "UF_CONTACT_METHOD":"contact_method",
        "PHONE": "phones",
        "EMAIL": "emails"
    }, inplace=True)

    logger.debug(f"Формируем кортежи для вставки в PostgreSQL")
    data_tuples = [tuple(row) for row in df.to_numpy()]
    logger.info(f"{len(data_tuples)} записей готовы для вставки")

    connection = BaseHook.get_connection('conn_pg')

    with pg.connect(
        dbname=PG_DATABASE,
        sslmode="disable",
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    ) as conn:

        try:
            with conn.cursor() as cur:

                # ON COMMIT DROP : таблица будет удалена после коммита транзакции.
                create_temp_table_query = """
                CREATE TEMP TABLE temp_leads (
                    lead_id INT,
                    title VARCHAR,
                    first_name VARCHAR,
                    last_name VARCHAR,
                    status_id VARCHAR,
                    source_id VARCHAR,
                    client_type VARCHAR,
                    contact_method VARCHAR,
                    phones JSONB,
                    emails JSONB
                ) ON COMMIT DROP; 
                """
                cur.execute(create_temp_table_query)

                insert_query = """
                INSERT INTO temp_leads (
                    lead_id, title, first_name, last_name, status_id, source_id, 
                    client_type, contact_method, phones, emails
                )
                VALUES %s;
                """
            
                template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)"

                execute_values(
                    cur, 
                    insert_query, 
                    data_tuples, 
                    template=template, 
                    page_size=5000  # размер батча
                )

                merge_query = """
                MERGE INTO leads AS target
                USING temp_leads AS src
                ON target.lead_id = src.lead_id
                WHEN MATCHED THEN 
                    UPDATE SET 
                        title = src.title,
                        first_name = src.first_name,
                        last_name = src.last_name,
                        status_id = src.status_id,
                        source_id = src.source_id,
                        client_type = src.client_type,
                        contact_method = src.contact_method,
                        phones = src.phones,
                        emails = src.emails
                WHEN NOT MATCHED THEN 
                    INSERT (lead_id, title, first_name, last_name, status_id, source_id, client_type, contact_method, phones, emails) 
                    VALUES (src.lead_id, src.title, src.first_name, src.last_name, src.status_id, src.source_id, src.client_type, src.contact_method, src.phones, src.emails)
                WHEN NOT MATCHED BY SOURCE THEN 
                    DELETE;
                """
                cur.execute(merge_query)
            
                conn.commit()

                logger.info("Merge выполнен успешно")

        except Exception as e:
            conn.rollback()
            logger.error(
                f"Ошибка при обновлении базы данных: {e}",
                exc_info=True
            )
            raise

