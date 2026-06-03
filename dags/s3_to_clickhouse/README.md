# S3 to ClickHouse — Weekly Visits

Еженедельный DAG для агрегации подневной статистики визитов из S3 (загружается DAG [`api_to_s3`](../api_to_s3/README.md)) и записи итога в распределённую таблицу ClickHouse.

## DAG

| Параметр | Значение |
|---|---|
| **dag_id** | `not_so_bad_weekly_visits_to_clickhouse` |
| **Расписание** | `0 6 * * 1` (каждый понедельник в 06:00) |
| **Владелец** | `not_so_bad` |
| **Теги** | `not_so_bad`, `metrika`, `clickhouse` |
| **start_date** | 2026-05-14 |
| **retries** | 2 (задержка 10 мин) |
| **max_active_runs** | 1 |

## Граф задач

```
dag_start → load_from_s3 → upload_to_clickhouse → dag_end
```

### load_from_s3

Читает parquet-файлы за предыдущую календарную неделю (пн–вс) из S3 и суммирует колонку `visits`.

| Параметр | Значение |
|---|---|
| **Connection** | `minios3_conn` |
| **Bucket** | `dev` |
| **Key** | `not_so_bad/metrika/traffic_data_{ds}.parquet` |

### upload_to_clickhouse

Удаляет существующую запись за ту же неделю (для идемпотентности) и вставляет агрегат в распределённую таблицу `weekly_visits_stats`.

## Необходимая конфигурация

### Airflow Connections

| Connection ID | Тип | Описание |
|---|---|---|
| `minios3_conn` | AWS / S3 | Подключение к S3 (те же файлы, что пишет `api_to_s3`) |
| `conn_clickhouse` | HTTP | Подключение к ClickHouse (host, port=8123, login, password, schema) |

### ClickHouse

Перед первым запуском создайте локальную и распределённую таблицы на кластере — см. [`sql/create_tables.sql`](sql/create_tables.sql). 

INSERT выполняется в `weekly_visits_stats` (Distributed)

## Зависимости

- `pandas`
- `pyarrow`
- `clickhouse-connect`
- `apache-airflow-providers-amazon`
