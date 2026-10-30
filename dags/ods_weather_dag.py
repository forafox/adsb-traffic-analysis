"""Погода METAR по аэропортам ЮАР: сбор сводок и загрузка в ODS."""

import os
from datetime import datetime

from airflow.sdk import dag, task

import datasets
import ods

AIRPORTS = ["FACT", "FAOR", "FALE", "FAPE"]
HOURS = 12


@dag(dag_id="ods_weather", schedule=os.environ.get("WEATHER_SCHEDULE") or None,
     start_date=datetime(2026, 9, 27), catchup=False, tags=["ods", "weather"])
def ods_weather():
    @task
    def extract() -> str:
        from collect_weather import collect

        path, failed = collect(AIRPORTS, HOURS)
        if not path:
            raise RuntimeError("METAR не отдал сводок")
        return path

    @task(outlets=[datasets.ODS_METAR])
    def load(path: str) -> int:
        return ods.load("metar", ods.read_metar(path))

    load(extract())


ods_weather()
