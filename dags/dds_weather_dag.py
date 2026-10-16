"""DDS: погода METAR. ODS -> очищенные наблюдения со ссылкой на аэропорт."""

from datetime import datetime

from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import dag, task

import datasets
import dds


@dag(dag_id="dds_weather", schedule=[datasets.ODS_METAR], start_date=datetime(2026, 9, 27),
     catchup=False, tags=["dds", "weather"])
def dds_weather():
    ods_ready = PythonSensor(task_id="ods_ready", python_callable=dds.ods_ready,
                             op_args=["ods_metar"], poke_interval=30, timeout=600,
                             mode="reschedule")

    @task
    def schema():
        dds.ensure_schema()

    @task
    def fact_weather() -> int:
        return dds.run_script("60_fact_weather.sql", "dds_fact_weather")

    @task(outlets=[datasets.DDS_WEATHER])
    def publish():
        print("Погода в DDS обновлена")

    ods_ready >> schema() >> fact_weather() >> publish()


dds_weather()
