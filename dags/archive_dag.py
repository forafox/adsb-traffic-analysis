"""Разовая загрузка в ODS всего, что собрано вручную до появления Airflow."""

import glob
import os
import sys
from datetime import datetime

from airflow.sdk import dag, task

sys.path.insert(0, os.environ.get("PROJECT_DIR", "/opt/project"))

import ods


def load_everywhere(dataset, rows):
    return sum(ods.load(dataset, storage, rows) for storage in ods.LOADERS)


@dag(dag_id="ods_archive", schedule=None, start_date=datetime(2026, 9, 27),
     catchup=False, tags=["ods", "backfill"])
def ods_archive():
    @task
    def positions() -> int:
        run_ids = sorted(os.path.basename(os.path.dirname(p))
                         for p in glob.glob(os.path.join(ods.RAW_DIR, "*", "manifest.json")))
        print(f"Снимков позиций: {len(run_ids)}")
        return sum(load_everywhere("positions", ods.read_positions(r)) for r in run_ids)

    @task
    def flights() -> int:
        paths = sorted(glob.glob(os.path.join(ods.RAW_DIR, "board", "*.json")))
        print(f"Снимков табло: {len(paths)}")
        return sum(load_everywhere("flights", ods.read_flights(p)) for p in paths)

    @task
    def metar() -> int:
        paths = sorted(glob.glob(os.path.join(ods.RAW_DIR, "weather", "*.json")))
        print(f"Снимков погоды: {len(paths)}")
        return sum(load_everywhere("metar", ods.read_metar(p)) for p in paths)

    positions()
    flights()
    metar()


ods_archive()
