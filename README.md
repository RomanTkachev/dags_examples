# dags_examples

Репозиторий-портфолио с примерами DAG для [Apache Airflow](https://airflow.apache.org/). Каждая папка в `dags/` — отдельный сценарий оркестрации: загрузка из внешних API, трансформации в PostgreSQL, выгрузка в S3 и ClickHouse, в том числе SCD Type 2 на Spark.

## Структура

```
dags/
├── api_to_s3/                          # API → S3 (Parquet)
├── api_to_postgre/                     # API → PostgreSQL
├── postgre_to_postrge/                 # агрегация PostgreSQL → S3
├── s3_to_clickhouse/                   # S3 → ClickHouse
└── postgres_to_clickhouse_spark_scd2/  # PostgreSQL → ClickHouse, SCD2 (Spark)
```

Подробности по расписанию, connections, таблицам и зависимостям — в `README.md` внутри соответствующей папки.

## Общие принципы

- DAG описаны как Python-модули с задачами `PythonOperator` (и `EmptyOperator` для границ графа).
- Секреты и endpoints — через Airflow Connections и Variables, не в коде.

## Зависимости

Набор библиотек зависит от DAG (например: `pandas`, `psycopg2`, `boto3`, `clickhouse-connect`, `pyspark`). См. README в нужной папке.
