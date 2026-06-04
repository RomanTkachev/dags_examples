# PostgreSQL → ClickHouse — SCD Type 2 (Spark)

Ежедневный DAG: полный срез справочника `users` из PostgreSQL через Spark, сравнение с текущими версиями в `users_history` (ClickHouse), закрытие изменившихся записей и вставка новых версий.

## DAG

| Параметр | Значение |
|---|---|
| **dag_id** | `not_so_bad_users_scd2_to_clickhouse` |
| **Расписание** | `0 6 * * *` (каждый день в 06:00) |
| **Владелец** | `not_so_bad` |
| **Теги** | `not_so_bad`, `postgres`, `spark`, `clickhouse`, `scd2` |
| **start_date** | 2026-05-14 |
| **retries** | 2 (задержка 10 мин) |
| **max_active_runs** | 1 |

## Граф задач

```
dag_start → load_from_postgres → apply_scd2 → upload_to_clickhouse → dag_end
```

### load_from_postgres

Читает полный срез таблицы `users` из PostgreSQL (JDBC + Spark) и сохраняет parquet во временный каталог `/tmp/users_scd2/{ds}/source`.

| Параметр | Значение |
|---|---|
| **Connection** | `conn_pg` |
| **БД** | `etl` |
| **Таблица** | `users` |

Схема источника: `customer_id`, `name`, `city`, `phone`.

### apply_scd2

Загружает срез из parquet и текущие строки (`is_current = true`) из `users_history` в ClickHouse, находит:

- **новых** клиентов (есть в source, нет в истории);
- **изменившихся** (атрибуты `name`, `city`, `phone` отличаются от текущей версии).

Для изменившихся формирует список `customer_key` для закрытия. Для новых и изменившихся — новые версии с `effective_from = ds`, `effective_to = 9999-12-31`, `is_current = true`. Суррогатный ключ: `max(customer_key) + row_number`.

Дата закрытия старых версий: `ds - 1 день`.

### upload_to_clickhouse

- `ALTER TABLE ... UPDATE` — закрытие старых версий (`effective_to`, `is_current = false`);
- `INSERT` — новые версии в `users_history`.

## Схема users_history (ClickHouse)

| Поле | Тип | Описание |
|---|---|---|
| customer_key | BIGINT | Суррогатный ключ версии |
| customer_id | STRING | Бизнес-ключ |
| name | STRING | Имя |
| city | STRING | Город |
| phone | STRING | Телефон |
| effective_from | DATE | Начало действия версии |
| effective_to | DATE | Конец (`9999-12-31` для текущей) |
| is_current | BOOLEAN | Признак текущей версии |

## Необходимая конфигурация

### Airflow Connections

| Connection ID | Тип | Описание |
|---|---|---|
| `conn_pg` | Postgres | PostgreSQL, БД `etl`, таблица `users` |
| `conn_clickhouse` | HTTP | ClickHouse (host, port=8123, login, password, schema) |

### Переменные окружения (опционально)

| Переменная | Описание |
|---|---|
| `SPARK_MASTER` | URL Spark (по умолчанию `local[*]`) |
| `SPARK_JARS` | JDBC-драйверы: `postgresql-*.jar`, `clickhouse-jdbc-*.jar` |

### ClickHouse

Перед первым запуском создайте таблицы — см. [`sql/create_tables.sql`](sql/create_tables.sql).

INSERT и мутации выполняются в распределённую таблицу `users_history`.

## Структура папки

```
postgres_to_clickhouse_spark_scd2/
├── users_scd2_daily.py          # определение DAG
├── src/
│   ├── spark_session.py         # SparkSession
│   ├── load_from_postgres.py    # выгрузка users из PostgreSQL
│   ├── apply_scd2.py            # сравнение и расчёт изменений
│   └── upload_to_clickhouse.py  # закрытие и вставка в ClickHouse
├── sql/
│   └── create_tables.sql
└── README.md
```

## Зависимости

- `pyspark`
- `clickhouse-connect`
- `pendulum`
- JDBC: PostgreSQL, ClickHouse (JAR на воркерах Spark / `SPARK_JARS`)
