"""Построение слоя DDS в ClickHouse: запуск SQL-скриптов из sql/dds.

Вся логика очистки, обогащения и слияния лежит в SQL-скриптах, здесь только запуск.
Скрипты идемпотентны, поэтому любой DAG можно перезапускать.
"""

import os

from clickhouse import client

PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/project")
SQL_DIR = os.path.join(PROJECT_DIR, "sql", "dds")


def statements(name):
    with open(os.path.join(SQL_DIR, name), encoding="utf-8") as f:
        lines = [line for line in f if not line.lstrip().startswith("--")]
    return [s.strip() for s in "".join(lines).split(";") if s.strip()]


def count(ch, table):
    return ch.query(f"select count() from {table}").result_rows[0][0]


def run_script(name, table=None):
    """Выполняет скрипт; если указана таблица, возвращает, на сколько строк она выросла."""
    ch = client()
    before = count(ch, table) if table else 0
    for statement in statements(name):
        ch.command(statement)
    added = count(ch, table) - before if table else 0
    print(f"{name}: " + (f"{table} +{added} строк" if table else "выполнен"))
    return added


# статические измерения наполняют все три DAGа DDS, и при одновременном запуске
# проверка «такой строки ещё нет» не спасает от гонки, поэтому дубли схлопываются явно
STATIC_DIMS = ["dds_dim_date", "dds_dim_source", "dds_dim_region", "dds_dim_airline",
               "dds_dim_airport", "dds_dim_aircraft_type", "dds_dim_aircraft"]


def ensure_schema():
    run_script("00_schema.sql")
    run_script("10_static.sql")
    ch = client()
    for table in STATIC_DIMS:
        ch.command(f"optimize table {table} final")


def ods_ready(table):
    """Для сенсора: таблица ODS существует и в ней есть данные."""
    try:
        return count(client(), table) > 0
    except Exception as e:
        print(f"{table} пока недоступна: {type(e).__name__}")
        return False
