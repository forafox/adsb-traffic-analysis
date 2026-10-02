"""Позиции воздушных судов: опрос трёх API и загрузка снимка в три хранилища."""

import os
import sys
from datetime import datetime

from airflow.sdk import dag, task

sys.path.insert(0, os.environ.get("PROJECT_DIR", "/opt/project"))

import ods

REGIONS = ["cpt", "za"]
SOURCES = ["opensky", "adsblol", "adsbfi"]


@dag(dag_id="ods_positions", schedule=os.environ.get("POSITIONS_SCHEDULE") or None,
     start_date=datetime(2026, 9, 27), catchup=False, tags=["ods", "adsb"])
def ods_positions():
    @task
    def extract() -> str:
        from collect import collect

        snapshot, failed = collect(REGIONS, SOURCES)
        if failed:
            print(f"Источников с ошибкой: {failed}, грузим то, что собралось")
        return snapshot

    @task
    def load(storage: str, snapshot: str) -> int:
        return ods.load("positions", storage, ods.read_positions(snapshot))

    load.partial(snapshot=extract()).expand(storage=list(ods.LOADERS))


ods_positions()
