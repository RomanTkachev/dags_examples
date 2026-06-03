# Postgre to Postgre — агрегация API-данных

Ежедневный DAG для агрегации данных из таблицы с сырыми API-записями в PostgreSQL и выгрузки результата в S3 в формате CSV.

## DAG

| Параметр | Значение |
|---|---|
| **dag_id** | `tkachev_combine_api_data` |
| **Расписание** | `@daily` |
| **Владелец** | `tkachev` |
| **Теги** | `tkachev` |
| **start_date** | 2026-05-14 |
| **retries** | 2 (задержка 10 мин) |
| **max_active_runs** | 1 |

## Граф задач

```
dag_start → combine_data → upload_data → dag_end
```

### combine_data

Выполняет агрегирующий `INSERT` в PostgreSQL (БД `etl`) за дату выполнения (`ds`):

- **Источник:** `tkachev_api_table` — записи с `created_at` в интервале `[ds, ds + 1 день)`
- **Приёмник:** `tkachev_agg_table`

Агрегация по `lti_user_id` и `attempt_type`:

| Поле | Описание |
|---|---|
| `lti_user_id` | ID пользователя |
| `attempt_type` | Тип попытки |
| `count(1)` | Общее число попыток |
| `attempt_failed_count` | Число неуспешных попыток (`is_correct = false`) |
| `date` | Дата выполнения DAG (`ds`) |

### upload_data

Читает агрегированные данные из `tkachev_agg_table` за дату `ds`, сериализует в TSV (разделитель `\t`, UTF-8) и загружает в S3.

| Параметр | Значение |
|---|---|
| **Connection** | `conn_s3` |
| **Bucket** | `default-storage` |
| **Key** | `tkachev_{ds}.csv` |

## Необходимая конфигурация

### Airflow Connections

| Connection ID | Тип | Описание |
|---|---|---|
| `conn_pg` | Postgres | Подключение к БД `etl` |
| `conn_s3` | AWS / S3 | Подключение к S3-совместимому хранилищу |

### Таблицы PostgreSQL

**tkachev_api_table** — сырые данные из API:

- `lti_user_id`, `attempt_type`, `is_correct`, `created_at`

**tkachev_agg_table** — агрегированные данные:

- `lti_user_id`, `attempt_type`, количество попыток, `attempt_failed_count`, `date`

## Структура папки

```
postgre_to_postrge/
├── tkachev_combine_api_data.py   # определение DAG
├── src/
│   ├── combine_data.py           # агрегация в PostgreSQL
│   └── upload_data.py            # выгрузка в S3
└── README.md
```

## Зависимости

- `psycopg2`
- `boto3`
