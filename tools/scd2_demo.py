#!/usr/bin/env python3
"""Проверка механизма SCD2 на синтетическом борте.

В собранных данных атрибуты бортов за неделю не менялись, поэтому в dds_dim_aircraft
у каждого борта одна открытая версия. Чтобы показать, что версии действительно
закрываются, тот же самый SQL из sql/dds/20_dim_aircraft.sql выполняется на временных
таблицах, куда подставлен борт со сменой регистрации и оператора.

Запуск внутри контейнера Airflow, где есть драйвер:

    docker compose exec airflow-scheduler python /opt/project/tools/scd2_demo.py

Временные таблицы удаляются в конце.
"""

import json
import os
import sys

sys.path.insert(0, "/opt/airflow/dags")

import dds

ODS = "scd2_demo_ods"
DIM = "scd2_demo_dim"
ICAO = "00dem0"

# три снимка одного борта: во втором атрибуты те же, в третьем сменились регистрация и оператор
SNAPSHOTS = [
    ("20261001T060000Z", "2026-10-01T06:00:00Z", "ZS-AAA", "B738", "Old Operator"),
    ("20261001T120000Z", "2026-10-01T12:00:00Z", "ZS-AAA", "B738", "Old Operator"),
    ("20261003T090000Z", "2026-10-03T09:00:00Z", "ZS-BBB", "B738", "New Operator"),
]


def dim_ddl():
    """Берём DDL измерения из рабочей схемы, чтобы демонстрация шла на той же структуре."""
    for statement in dds.statements("00_schema.sql"):
        if "dds_dim_aircraft (" in statement:
            return statement.replace("dds_dim_aircraft", DIM)
    raise RuntimeError("в 00_schema.sql не найдено объявление dds_dim_aircraft")


def scd2_insert():
    """Рабочий SQL построения SCD2 с подменёнными именами таблиц."""
    for statement in dds.statements("20_dim_aircraft.sql"):
        if "insert into dds_dim_aircraft\n" in statement + "\n" and "with" in statement:
            return statement.replace("dds_dim_aircraft", DIM).replace("ods_positions", ODS)
    raise RuntimeError("в 20_dim_aircraft.sql не найден insert в dds_dim_aircraft")


def main():
    ch = dds.client()
    ch.command(f"drop table if exists {ODS}")
    ch.command(f"drop table if exists {DIM}")
    ch.command(f"create table {ODS} as ods_positions")
    ch.command(dim_ddl())

    rows = [[run_id, "adsblol", "cpt", fetched_at, ICAO, "DEMO1", -33.9, 18.6, 1000.0,
             0, 100.0, 90.0, json.dumps({"r": reg, "t": typ, "ownOp": op})]
            for run_id, fetched_at, reg, typ, op in SNAPSHOTS]
    ch.insert(ODS, rows, column_names=["run_id", "source", "region", "fetched_at", "icao24",
                                       "callsign", "lat", "lon", "alt_m", "on_ground",
                                       "speed_ms", "track", "raw"])
    ch.command(scd2_insert())

    print(f"Снимков на входе: {len(SNAPSHOTS)}, из них с изменением атрибутов: 1\n")
    print(f"{'регистрация':13s}{'оператор':15s}{'действует с':22s}{'действует по':22s}текущая")
    for reg, op, start, end, current in ch.query(
            f"select registration, operator, valid_from, valid_to, is_current "
            f"from {DIM} final where icao24 = '{ICAO}' order by valid_from").result_rows:
        print(f"{reg:13s}{op:15s}{str(start):22s}{str(end):22s}{current}")

    versions = ch.query(f"select count(), countIf(is_current = 0), countIf(is_current = 1) "
                        f"from {DIM} final where icao24 = '{ICAO}'").result_rows[0]
    ch.command(f"drop table {ODS}")
    ch.command(f"drop table {DIM}")

    ok = versions == (2, 1, 1)
    print(f"\nВерсий: {versions[0]}, закрытых: {versions[1]}, текущих: {versions[2]}")
    print("SCD2 работает: смена атрибутов закрыла прошлую версию и открыла новую" if ok
          else "Ожидались две версии: одна закрытая и одна текущая")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
