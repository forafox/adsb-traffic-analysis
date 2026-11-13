#!/usr/bin/env python3
"""Загрузка каталога и построения Data Lineage в OpenMetadata.

Скрипт регистрирует три слоя хранилища и файлы сырых снимков, подтягивает состав
столбцов прямо из ClickHouse и соединяет сущности рёбрами происхождения: от файлов,
которые кладут сборщики, через ODS и DDS до витрин.

Сбор метаданных агентом OpenMetadata не используется: его контейнер — это второй
Airflow, который на учебном стенде не нужен. Всё делается через REST API.

Запуск на хосте, когда подняты ClickHouse и OpenMetadata:

    docker compose -f docker-compose.openmetadata.yml up -d
    python3 tools/openmetadata_lineage.py

Повторный запуск перезаписывает сущности и рёбра, дублей не возникает.
"""

import base64
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

OM = "http://localhost:8585/api/v1"
OM_USER, OM_PASSWORD = "admin@open-metadata.org", "admin"

CLICKHOUSE = "http://localhost:8123"
CH_USER, CH_PASSWORD, CH_DATABASE = "ods", "ods", "ods"

FILES_SERVICE = "adsb-raw-files"
DB_SERVICE = "adsb-clickhouse"
SCHEMA_FQN = f"{DB_SERVICE}.{CH_DATABASE}.{CH_DATABASE}"

# Файлы, которые кладут сборщики. В OpenMetadata это контейнеры хранилища.
CONTAINERS = {
    "positions": "Снимки позиций бортов из OpenSky, adsb.lol и adsb.fi, data/raw/positions",
    "board": "Снимки табло аэропорта Кейптауна, data/raw/board",
    "weather": "Сводки METAR по аэропортам ЮАР, data/raw/weather",
}

TABLES = {
    "ods_positions": "Сырые позиции бортов: разобраны только поля для запросов, ответ целиком в raw",
    "ods_flights": "Сырые рейсы с табло: расписание, факт и статус",
    "ods_metar": "Сырые сводки погоды METAR",
    "dds_dim_date": "Календарь: день недели, квартал, признак выходного",
    "dds_dim_source": "Источники ADS-B, номер ключа задаёт приоритет при слиянии",
    "dds_dim_region": "Зоны опроса: Кейптаун и вся ЮАР",
    "dds_dim_airline": "Авиакомпании с табло, SCD1",
    "dds_dim_airport": "Аэропорты с табло и коды ICAO станций METAR, SCD1",
    "dds_dim_aircraft_type": "Типы самолётов, вынесены из измерения бортов ради 3НФ",
    "dds_dim_aircraft": "Борта с историей версий, SCD2",
    "dds_fact_position": "Очищенная позиция борта: снимок, источник, регион, борт",
    "dds_fact_aircraft_snapshot": "Борт в снимке после слияния трёх сетей",
    "dds_fact_flight": "Рейс за день с рассчитанной задержкой",
    "dds_fact_weather": "Наблюдение METAR без перекрытий",
    "dds_dq_results": "Журнал проверок качества",
    "dm_airport_load_hourly": "Витрина: загрузка аэропорта по часам суток",
    "dm_airline_punctuality": "Витрина: пунктуальность авиакомпаний",
    "dm_source_coverage": "Витрина: покрытие источников ADS-B",
    "dm_weather_runway": "Витрина: роза ветров и рабочая полоса",
    "dm_fleet_mix": "Витрина: состав наблюдаемого парка",
    "dm_traffic_daily": "Витрина: трафик по дням",
}

# Рёбра происхождения: откуда, куда и чем преобразовано.
LINEAGE = [
    ("positions", "ods_positions", "dags/ods_positions_dag.py: разбор JSON в колонки"),
    ("board", "ods_flights", "dags/ods_board_dag.py: разбор карточек рейсов"),
    ("weather", "ods_metar", "dags/ods_weather_dag.py: разбор сводок METAR"),

    ("ods_positions", "dds_dim_aircraft", "sql/dds/20_dim_aircraft.sql: версии борта по SCD2"),
    ("ods_positions", "dds_dim_aircraft_type", "sql/dds/20_dim_aircraft.sql: справочник типов"),
    ("ods_positions", "dds_fact_position", "sql/dds/30_fact_position.sql: очистка и обогащение"),
    ("dds_dim_aircraft", "dds_fact_position", "sql/dds/30_fact_position.sql: версия на момент снимка"),
    ("dds_dim_source", "dds_fact_position", "sql/dds/30_fact_position.sql: ключ источника"),
    ("dds_dim_date", "dds_fact_position", "sql/dds/30_fact_position.sql: ключ даты"),
    ("dds_fact_position", "dds_fact_aircraft_snapshot",
     "sql/dds/31_fact_aircraft_snapshot.sql: слияние трёх сетей"),

    ("ods_flights", "dds_dim_airline", "sql/dds/40_dim_airline_airport.sql: справочник, SCD1"),
    ("ods_flights", "dds_dim_airport", "sql/dds/40_dim_airline_airport.sql: справочник, SCD1"),
    ("ods_flights", "dds_fact_flight", "sql/dds/50_fact_flight.sql: последнее состояние и задержка"),
    ("dds_dim_airline", "dds_fact_flight", "sql/dds/50_fact_flight.sql: ключ авиакомпании"),
    ("dds_dim_airport", "dds_fact_flight", "sql/dds/50_fact_flight.sql: ключ аэропорта"),
    ("dds_dim_date", "dds_fact_flight", "sql/dds/50_fact_flight.sql: ключ даты"),

    ("ods_metar", "dds_fact_weather", "sql/dds/60_fact_weather.sql: снятие перекрытий"),
    ("dds_dim_airport", "dds_fact_weather", "sql/dds/60_fact_weather.sql: ключ аэропорта"),

    ("dds_fact_position", "dds_dq_results", "etl/dq.py: область positions, 9 проверок"),
    ("dds_fact_flight", "dds_dq_results", "etl/dq.py: область flights, 7 проверок"),
    ("dds_fact_weather", "dds_dq_results", "etl/dq.py: область weather, 6 проверок"),

    ("dds_fact_flight", "dm_airport_load_hourly", "sql/dm/10_airport_load.sql"),
    ("dds_fact_flight", "dm_airline_punctuality", "sql/dm/20_airline_punctuality.sql"),
    ("dds_dim_airline", "dm_airline_punctuality", "sql/dm/20_airline_punctuality.sql"),
    ("dds_fact_position", "dm_source_coverage", "sql/dm/30_source_coverage.sql"),
    ("dds_dim_source", "dm_source_coverage", "sql/dm/30_source_coverage.sql"),
    ("dds_fact_weather", "dm_weather_runway", "sql/dm/40_weather_runway.sql"),
    ("dds_fact_aircraft_snapshot", "dm_fleet_mix", "sql/dm/50_fleet_mix.sql"),
    ("dds_dim_aircraft", "dm_fleet_mix", "sql/dm/50_fleet_mix.sql"),
    ("dds_dim_aircraft_type", "dm_fleet_mix", "sql/dm/50_fleet_mix.sql"),
    ("dds_fact_flight", "dm_traffic_daily", "sql/dm/60_traffic_daily.sql"),
    ("dds_fact_aircraft_snapshot", "dm_traffic_daily", "sql/dm/60_traffic_daily.sql"),
    ("dds_fact_weather", "dm_traffic_daily", "sql/dm/60_traffic_daily.sql"),
    ("dds_dim_date", "dm_traffic_daily", "sql/dm/60_traffic_daily.sql"),
]

# ClickHouse знает больше типов, чем каталог; сводим к тем, что понимает OpenMetadata.
TYPES = [("String", "STRING"), ("UUID", "STRING"), ("UInt64", "BIGINT"), ("Int64", "BIGINT"),
         ("UInt32", "INT"), ("Int32", "INT"), ("UInt16", "INT"), ("Int16", "INT"),
         ("UInt8", "INT"), ("Int8", "INT"), ("Float64", "DOUBLE"), ("Float32", "FLOAT"),
         ("DateTime", "DATETIME"), ("Date", "DATE")]


def om(method, path, token, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(OM + path, data=data, method=method, headers={
        "Content-Type": "application/json", "Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read().decode()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {path} -> HTTP {e.code}: {e.read().decode()[:300]}") from None


def login():
    password = base64.b64encode(OM_PASSWORD.encode()).decode()
    request = urllib.request.Request(
        OM + "/users/login", method="POST",
        data=json.dumps({"email": OM_USER, "password": password}).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode())["accessToken"]


def clickhouse(query):
    url = f"{CLICKHOUSE}/?{urllib.parse.urlencode({'database': CH_DATABASE, 'query': query})}"
    request = urllib.request.Request(url, headers={
        "X-ClickHouse-User": CH_USER, "X-ClickHouse-Key": CH_PASSWORD})
    with urllib.request.urlopen(request, timeout=30) as response:
        return [line.split("\t") for line in response.read().decode().strip().split("\n") if line]


def om_type(clickhouse_type):
    for prefix, mapped in TYPES:
        if prefix in clickhouse_type:
            return mapped
    return "STRING"


def columns_by_table():
    rows = clickhouse(
        "select table, name, type from system.columns "
        f"where database = '{CH_DATABASE}' order by table, position")
    columns = {}
    for table, name, type_ in rows:
        columns.setdefault(table, []).append({
            "name": name, "dataType": om_type(type_),
            "dataTypeDisplay": type_, "dataLength": 1})
    return columns


def create_services(token):
    om("PUT", "/services/storageServices", token, {
        "name": FILES_SERVICE,
        "description": "Сырые снимки, которые складывают сборщики в data/raw",
        "serviceType": "CustomStorage",
        "connection": {"config": {"type": "CustomStorage"}}})
    om("PUT", "/services/databaseServices", token, {
        "name": DB_SERVICE,
        "description": "ClickHouse: сырой слой, детальный слой и витрины",
        "serviceType": "Clickhouse",
        "connection": {"config": {
            "type": "Clickhouse", "scheme": "clickhouse+http",
            "username": CH_USER, "password": CH_PASSWORD,
            "hostPort": "clickhouse:8123", "databaseSchema": CH_DATABASE}}})
    om("PUT", "/databases", token, {"name": CH_DATABASE, "service": DB_SERVICE,
                                    "description": "База ods: все три слоя лежат рядом"})
    om("PUT", "/databaseSchemas", token, {"name": CH_DATABASE,
                                          "database": f"{DB_SERVICE}.{CH_DATABASE}"})


def create_entities(token, columns):
    entities = {}
    for name, description in CONTAINERS.items():
        answer = om("PUT", "/containers", token, {
            "name": name, "service": FILES_SERVICE, "description": description,
            "fileFormats": ["json"], "prefix": f"/data/raw/{name}"})
        entities[name] = ("container", answer["id"])

    missing = [t for t in TABLES if t not in columns]
    if missing:
        raise RuntimeError(f"В ClickHouse нет таблиц: {', '.join(missing)}")
    for name, description in TABLES.items():
        answer = om("PUT", "/tables", token, {
            "name": name, "databaseSchema": SCHEMA_FQN,
            "description": description, "columns": columns[name]})
        entities[name] = ("table", answer["id"])
    return entities


def create_lineage(token, entities):
    for source, target, description in LINEAGE:
        from_type, from_id = entities[source]
        to_type, to_id = entities[target]
        om("PUT", "/lineage", token, {"edge": {
            "fromEntity": {"id": from_id, "type": from_type},
            "toEntity": {"id": to_id, "type": to_type},
            "lineageDetails": {"description": description}}})


def main():
    token = login()
    columns = columns_by_table()
    create_services(token)
    entities = create_entities(token, columns)
    print(f"Зарегистрировано: {len(CONTAINERS)} контейнеров, {len(TABLES)} таблиц")
    create_lineage(token, entities)
    print(f"Построено рёбер происхождения: {len(LINEAGE)}")
    print(f"\nГраф: http://localhost:8585/table/{DB_SERVICE}.{CH_DATABASE}."
          f"{CH_DATABASE}.dm_traffic_daily/lineage")
    print("Вход admin@open-metadata.org / admin")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as e:
        print(f"Ошибка: {e}")
        sys.exit(1)
