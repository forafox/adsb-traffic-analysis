"""Слой DDS: подготовка схемы и ожидание данных в слое ODS.

Вся логика очистки, обогащения и слияния лежит в SQL-скриптах sql/dds,
здесь только их запуск в нужном порядке.
"""

import sql
from clickhouse import client

LAYER = "dds"

# статические измерения наполняют все три DAGа DDS, и при одновременном запуске
# проверка «такой строки ещё нет» не спасает от гонки, поэтому дубли схлопываются явно
STATIC_DIMS = ["dds_dim_date", "dds_dim_source", "dds_dim_region", "dds_dim_airline",
               "dds_dim_airport", "dds_dim_aircraft_type", "dds_dim_aircraft"]


def run_script(name, table=None):
    return sql.run_script(LAYER, name, table)


def ensure_schema():
    sql.run_script(LAYER, "00_schema.sql")
    sql.run_script(LAYER, "10_static.sql")
    ch = client()
    for table in STATIC_DIMS:
        ch.command(f"optimize table {table} final")

