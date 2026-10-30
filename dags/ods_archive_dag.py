"""Разовая загрузка в ODS всего, что собрано вручную до появления Airflow."""

import glob
import os
from datetime import datetime

from airflow.sdk import dag, task

import datasets
import ods


@dag(dag_id="ods_archive", schedule=None, start_date=datetime(2026, 9, 27),
     catchup=False, tags=["ods", "backfill"])
def ods_archive():
    @task(outlets=[datasets.ODS_POSITIONS])
    def positions() -> int:
        run_ids = sorted(os.path.basename(os.path.dirname(p))
                         for p in glob.glob(os.path.join(ods.POSITIONS_DIR, "*", "manifest.json")))
        print(f"Снимков позиций: {len(run_ids)}")
        return sum(ods.load("positions", ods.read_positions(r)) for r in run_ids)

    @task(outlets=[datasets.ODS_FLIGHTS])
    def flights() -> int:
        paths = sorted(glob.glob(os.path.join(ods.BOARD_DIR, "*.json")))
        print(f"Снимков табло: {len(paths)}")
        return sum(ods.load("flights", ods.read_flights(p)) for p in paths)

    @task(outlets=[datasets.ODS_METAR])
    def metar() -> int:
        paths = sorted(glob.glob(os.path.join(ods.WEATHER_DIR, "*.json")))
        print(f"Снимков погоды: {len(paths)}")
        return sum(ods.load("metar", ods.read_metar(p)) for p in paths)

    positions()
    flights()
    metar()


ods_archive()
