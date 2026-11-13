"""Витрины данных: пересборка слоя dm из детального слоя.

Стартует, когда обновились все три потока DDS. Каждая витрина строится своим скриптом
и независима от остальных, поэтому задачи идут параллельно. Последняя задача проверяет,
что ни одна витрина не осталась пустой, и только после этого публикует asset.
"""

from datetime import datetime

from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import dag, task

import datasets
import sql

LAYER = "dm"

MART_NAMES = ["загрузка по часам", "пунктуальность", "покрытие источников",
              "ветер и полосы", "состав парка", "трафик по дням"]


@dag(dag_id="dm_marts",
     schedule=[datasets.DDS_POSITIONS, datasets.DDS_FLIGHTS, datasets.DDS_WEATHER],
     start_date=datetime(2026, 9, 27), catchup=False, tags=["dm", "marts"])
def dm_marts():
    dds_ready = PythonSensor(task_id="dds_ready", python_callable=sql.has_rows,
                             op_args=["dds_fact_position"], poke_interval=30, timeout=600,
                             mode="reschedule")

    @task
    def schema():
        sql.run_script(LAYER, "00_schema.sql")

    @task
    def airport_load() -> int:
        return sql.run_script(LAYER, "10_airport_load.sql", "dm_airport_load_hourly")

    @task
    def airline_punctuality() -> int:
        return sql.run_script(LAYER, "20_airline_punctuality.sql", "dm_airline_punctuality")

    @task
    def source_coverage() -> int:
        return sql.run_script(LAYER, "30_source_coverage.sql", "dm_source_coverage")

    @task
    def weather_runway() -> int:
        return sql.run_script(LAYER, "40_weather_runway.sql", "dm_weather_runway")

    @task
    def fleet_mix() -> int:
        return sql.run_script(LAYER, "50_fleet_mix.sql", "dm_fleet_mix")

    @task
    def traffic_daily() -> int:
        return sql.run_script(LAYER, "60_traffic_daily.sql", "dm_traffic_daily")

    @task(outlets=[datasets.DM_MARTS])
    def verify(rows: list[int]):
        """Пустая витрина означает, что источник не доехал: об этом лучше узнать сразу."""
        empty = [n for n, r in zip(MART_NAMES, rows) if not r]
        if empty:
            raise RuntimeError(f"Витрины остались пустыми: {', '.join(empty)}")
        print("Витрины пересобраны: " + ", ".join(f"{n} {r}" for n, r in zip(MART_NAMES, rows)))

    marts = [airport_load(), airline_punctuality(), source_coverage(),
             weather_runway(), fleet_mix(), traffic_daily()]
    prepared = schema()
    dds_ready >> prepared
    for mart in marts:
        prepared >> mart
    verify(marts)


dm_marts()
