from airflow.hooks.base import BaseHook
from airflow.models import Variable

def load_from_api (**context):

    import requests
    import json 
    import pandas as pd
    from datetime import datetime
    import logging
    from requests.exceptions import RequestException, Timeout, ConnectionError as RequestsConnectionError

    URL = "https://api-metrika.yandex.net/stat/v1/data?preset=sources_summary"

    YANDEX_METRIKA_TOKEN = Variable.get("YANDEX_METRIKA_TOKEN")
    COUNTER_ID = Variable.get("YANDEX_METRIKA_COUNTER")

    params = {
        "ids": COUNTER_ID,
        "date1": context['ds'],
        "date2": context['ds'],
        "limit": 10000
    }

    headers = {
        "Authorization": f"OAuth {YANDEX_METRIKA_TOKEN}"
    }
    logger = logging.getLogger('airflow.task')

    logger.info(f"Попытка подключения к API для счетчика {COUNTER_ID}")
    logger.debug(f"Параметры запроса: url={URL}, params={params}")

    try:
        response = requests.get(URL, params=params, headers=headers, timeout=10)
        logger.debug(f"Получен ответ со статусом {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            logger.info(f"Успешное подключение к счетчику {COUNTER_ID}")
            logger.debug(f"Получены данные: {data}")
        else:
            logger.warning(
                    f"API вернул ошибку {response.status_code} для счетчика {COUNTER_ID}. "
                    f"Ответ: {response.text[:200]}"
                )
            response.raise_for_status()  # Выбросывает HTTPError

    except Timeout:
            logger.error(
                f"Таймаут подключения к API для счетчика {COUNTER_ID} (превышено 10 секунд)",
                exc_info=True
            )
            raise
            
    except RequestsConnectionError:
        # Ошибка соедниния 
        logger.error(
            f"Ошибка соединения с API для счетчика {COUNTER_ID}. "
            f"Проверьте сетевое подключение и доступность {URL}"
        )
        raise
        
    except RequestException as req_err:
        # Любая другая ошибка requests
        logger.error(
            f"Ошибка при запросе к API для счетчика {COUNTER_ID}: {req_err}"
        )
        raise

    logger.debug(f"Формируем датафрейм с данными")
    dimension_names = data["query"]["dimensions"]
    metric_names = data["query"]["metrics"]

    # Строим список строк
    rows = []
    for row in data["data"]:
        dim_values = [d["name"] for d in row["dimensions"]]
        metrics = row["metrics"]
        rows.append(dim_values + metrics)

    # Названия колонок
    columns = dimension_names + metric_names

    # Финальный DataFrame
    try:
        df = pd.DataFrame(rows, columns=columns)
        logger.info(f"Датафрейм сформирован")
    except Exception: 
        logger.error(
                f"Не удалось сформировать датафрейм",
                exc_info=True
            )
        raise

    df.drop(columns=["ym:s:lastSignDirectPlatformType"], inplace=True)
    df['update_at'] = context['ds']

    df.rename(columns={
        "ym:s:lastSignTrafficSource": "source_type",
        "ym:s:lastSignSourceEngine": "source",
        "ym:s:visits": "visits", 
        "ym:s:users": "users", 
        "ym:s:bounceRate": "bounce_rate", 
        "ym:s:pageDepth": "page_depth",
        "ym:s:avgVisitDurationSeconds": "avg_visit_duration_seconds",
    }, inplace=True)

    return df.to_dict()