# API to S3 — Yandex Metrika

Ежедневный DAG для загрузки статистики трафика из [Yandex Metrika API](https://yandex.ru/dev/metrika/doc/api2/api_v1/intro.html) и сохранения результата в S3 в формате Parquet.

## DAG

| Параметр | Значение |
|---|---|
| **dag_id** | `not_so_bad_tkachev_API_to_S3__metrika` |
| **Расписание** | `@daily` |
| **Владелец** | `not_so_bad` |
| **Теги** | `not_so_bad`, `metrika` |
| **start_date** | 2026-05-14 |
| **retries** | 2 (задержка 10 мин) |
| **max_active_runs** | 1 |

## Граф задач

```
dag_start → load_from_api → upload_to_s3 → dag_end
```

### load_from_api

Запрашивает отчёт `sources_summary` за дату выполнения (`ds`) через Metrika Reporting API:


### upload_to_s3

Читает DataFrame из XCom, сериализует в Parquet и загружает в S3 через `S3Hook`.

Если DataFrame пустой, загрузка не выполняется.

## Необходимая конфигурация

### Airflow Variables

| Variable | Описание |
|---|---|
| `YANDEX_METRIKA_TOKEN` | OAuth-токен для доступа к Metrika API |
| `YANDEX_METRIKA_COUNTER` | ID счётчика Яндекс.Метрики |

### Airflow Connections

| Connection ID | Тип | Описание |
|---|---|---|
| `minios3_conn` | AWS / S3 | Подключение к S3-совместимому хранилищу (MinIO) |

## Зависимости

- `requests`
- `pandas`
- `pyarrow` (для записи Parquet)
- `apache-airflow-providers-amazon`
