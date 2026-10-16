"""Data Quality слоя DDS: стартует, когда обновились все три потока DDS.

Четыре области проверяются параллельно. Если провалена хотя бы одна проверка уровня error,
итоговая задача падает, и DAG виден в интерфейсе красным.
"""

from datetime import datetime

from airflow.sdk import dag, task

import datasets
import dq


@dag(dag_id="dds_quality",
     schedule=[datasets.DDS_POSITIONS, datasets.DDS_FLIGHTS, datasets.DDS_WEATHER],
     start_date=datetime(2026, 9, 27), catchup=False, tags=["dds", "quality"])
def dds_quality():
    @task
    def check(area: str) -> int:
        return dq.run(area)

    @task
    def verdict(failed: list[int]):
        total = sum(failed)
        if total:
            raise RuntimeError(f"Проваленных проверок: {total}, подробности в логах задач "
                               f"и в таблице dds_dq_results")
        print("Все проверки качества пройдены")

    verdict(check.expand(area=list(dq.CHECKS)))


dds_quality()
