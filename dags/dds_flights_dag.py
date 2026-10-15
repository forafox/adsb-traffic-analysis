"""DDS: рейсы с табло. ODS -> авиакомпании и аэропорты (SCD1) -> факт рейса с задержкой."""

import os
import sys
from datetime import datetime

from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import dag, task

sys.path.insert(0, os.environ.get("PROJECT_DIR", "/opt/project"))

import datasets
import dds


@dag(dag_id="dds_flights", schedule=[datasets.ODS_FLIGHTS], start_date=datetime(2026, 9, 27),
     catchup=False, tags=["dds", "board"])
def dds_flights():
    ods_ready = PythonSensor(task_id="ods_ready", python_callable=dds.ods_ready,
                             op_args=["ods_flights"], poke_interval=30, timeout=600,
                             mode="reschedule")

    @task
    def schema():
        dds.ensure_schema()

    @task
    def dim_airline_airport():
        dds.run_script("40_dim_airline_airport.sql")

    @task
    def fact_flight() -> int:
        return dds.run_script("50_fact_flight.sql", "dds_fact_flight")

    @task(outlets=[datasets.DDS_FLIGHTS])
    def publish():
        print("Рейсы в DDS обновлены")

    ods_ready >> schema() >> dim_airline_airport() >> fact_flight() >> publish()


dds_flights()
