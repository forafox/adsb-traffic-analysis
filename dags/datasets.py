"""Assets Airflow: связывают DAGи слоёв ODS, DDS и проверки качества.

DAG слоя-потребителя стартует сам, когда DAG слоя-источника успешно обновил asset.
Так детальный слой ждёт завершения загрузки исходных данных без ручных запусков
и без привязки к расписанию.
"""

from airflow.sdk import Asset

ODS_POSITIONS = Asset("clickhouse://clickhouse/ods/ods_positions")
ODS_FLIGHTS = Asset("clickhouse://clickhouse/ods/ods_flights")
ODS_METAR = Asset("clickhouse://clickhouse/ods/ods_metar")

DDS_POSITIONS = Asset("clickhouse://clickhouse/ods/dds_fact_position")
DDS_FLIGHTS = Asset("clickhouse://clickhouse/ods/dds_fact_flight")
DDS_WEATHER = Asset("clickhouse://clickhouse/ods/dds_fact_weather")
