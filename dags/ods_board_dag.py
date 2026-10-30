"""Табло аэропорта Кейптауна: сбор рейсов за сутки и загрузка в ODS."""

import os
from datetime import datetime

from airflow.sdk import dag, task

import datasets
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
        # половина табло хуже отсутствия данных: молча загруженный неполный день
        # исказит анализ, поэтому задача падает и снимок остаётся незагруженным
        if failed:
            raise RuntimeError("табло отдало не все направления, снимок не загружаем")
        return path

    @task(outlets=[datasets.ODS_FLIGHTS])
    def load(path: str) -> int:
        return ods.load("flights", ods.read_flights(path))

    load(extract())


ods_board()
