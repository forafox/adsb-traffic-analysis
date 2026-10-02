"""Погода METAR по аэропортам ЮАР: сбор сводок и загрузка в три хранилища."""

import os
import sys
from datetime import datetime

from airflow.sdk import dag, task

sys.path.insert(0, os.environ.get("PROJECT_DIR", "/opt/project"))

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

    @task
    def load(storage: str, path: str) -> int:
        return ods.load("metar", storage, ods.read_metar(path))

    load.partial(path=extract()).expand(storage=list(ods.LOADERS))


ods_weather()
