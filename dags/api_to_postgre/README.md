# API to Postgre — ITResume Statistics

Ежедневный DAG для загрузки статистики попыток из [ITResume API](https://b2b.itresume.ru/api/statistics) и сохранения данных в PostgreSQL.

## DAG

| Параметр | Значение |
|---|---|
| **dag_id** | `tkachev_load_from_api_to_pg` |
| **Расписание** | `@daily` |
| **Владелец** | `tkachev` |
| **Теги** | `tkachev` |
| **start_date** | 2026-05-14 |
| **retries** | 2 (задержка 10 мин) |
| **max_active_runs** | 1 |

## Граф задач

```
dag_start → load_from_api → dag_end
```

### load_from_api

Запрашивает статистику за дату выполнения (`ds`) через ITResume API:

- **Endpoint:** `https://b2b.itresume.ru/api/statistics`
- **Параметры:** `client`, `client_key`, `start` (= `ds`), `end` (= `ds + 1 день`)

Каждая запись из ответа вставляется в таблицу `tkachev_api_table` (БД `etl`). Поле `passback_params` парсится и разбирается на отдельные колонки.

## Необходимая конфигурация

### Airflow Variables

| Variable | Описание |
|---|---|
| `ITRESUME_CLIENT` | Идентификатор клиента ITResume |
| `ITRESUME_CLIENT_KEY` | Ключ доступа к API |

### Airflow Connections

| Connection ID | Тип | Описание |
|---|---|---|
| `conn_pg` | Postgres | Подключение к БД `etl` |

### Таблицы PostgreSQL

**tkachev_api_table** — сырые данные из API:

## Зависимости

- `requests`
- `pendulum`
- `psycopg2`
