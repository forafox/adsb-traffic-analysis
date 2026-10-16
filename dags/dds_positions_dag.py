"""DDS: позиции бортов. ODS -> измерение самолёта (SCD2) -> очищенные позиции -> слияние источников."""

from datetime import datetime

from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import dag, task

import datasets
import dds


@dag(dag_id="dds_positions", schedule=[datasets.ODS_POSITIONS], start_date=datetime(2026, 9, 27),
     catchup=False, tags=["dds", "adsb"])
def dds_positions():
    ods_ready = PythonSensor(task_id="ods_ready", python_callable=dds.ods_ready,
                             op_args=["ods_positions"], poke_interval=30, timeout=600,
                             mode="reschedule")

    @task
    def schema():
        dds.ensure_schema()

    @task
    def dim_aircraft() -> int:
        dds.run_script("20_dim_aircraft.sql")
        return dds.count(dds.client(), "dds_dim_aircraft final")

    @task
    def fact_position() -> int:
        return dds.run_script("30_fact_position.sql", "dds_fact_position")

    @task
    def fact_aircraft_snapshot() -> int:
        return dds.run_script("31_fact_aircraft_snapshot.sql", "dds_fact_aircraft_snapshot")

    @task(outlets=[datasets.DDS_POSITIONS])
    def publish():
        print("Позиции в DDS обновлены")

    ods_ready >> schema() >> dim_aircraft() >> fact_position() >> fact_aircraft_snapshot() >> publish()


dds_positions()
