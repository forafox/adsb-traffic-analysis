"""Табло аэропорта Кейптауна: сбор рейсов за сутки и загрузка в три хранилища."""

import os
import sys
from datetime import datetime

from airflow.sdk import dag, task

sys.path.insert(0, os.environ.get("PROJECT_DIR", "/opt/project"))

import ods


@dag(dag_id="ods_board", schedule=os.environ.get("BOARD_SCHEDULE") or None,
     start_date=datetime(2026, 9, 27), catchup=False, tags=["ods", "board"])
def ods_board():
    @task
    def extract(logical_date=None) -> str:
        from collect_board import SAST, collect

        # при ручном запуске без расписания logical_date не передаётся
        day = (logical_date or datetime.now(SAST)).astimezone(SAST)
        path, failed = collect(day.strftime("%Y-%m-%d"))
        if not path:
            raise RuntimeError("табло не отдало ни одного рейса")
        return path

    @task
    def load(storage: str, path: str) -> int:
        return ods.load("flights", storage, ods.read_flights(path))

    load.partial(path=extract()).expand(storage=list(ods.LOADERS))


ods_board()
