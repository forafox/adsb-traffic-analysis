#!/usr/bin/env python3
"""Настройка Superset: подключение к ClickHouse, наборы данных, графики и дашборд.

Всё делается через REST API, поэтому дашборд воспроизводится одной командой
и не зависит от того, что кто-то накликал в интерфейсе.

Запуск на хосте, Superset должен быть поднят:

    python3 tools/superset_setup.py

Повторный запуск пересоздаёт графики и дашборд заново, источник и наборы данных
переиспользуются. Зависимостей нет, нужен только Python 3.9.
"""

import json
import sys
import urllib.error
import urllib.parse
import urllib.request

BASE = "http://localhost:8088"
USER, PASSWORD = "admin", "admin"

DB_NAME = "ClickHouse витрины ADS-B"
DB_URI = "clickhousedb://ods:ods@clickhouse:8123/ods"
SCHEMA = "ods"
DASHBOARD = "Воздушное движение Кейптауна"

MARTS = ["dm_airport_load_hourly", "dm_airline_punctuality", "dm_source_coverage",
         "dm_weather_runway", "dm_fleet_mix", "dm_traffic_daily"]


def metric(column, aggregate, label):
    return {"expressionType": "SIMPLE", "aggregate": aggregate, "label": label,
            "column": {"column_name": column}}


def where(subject, operator, comparator):
    return {"expressionType": "SIMPLE", "clause": "WHERE", "subject": subject,
            "operator": operator, "operatorId": operator, "comparator": comparator}


# Графики дашборда: таблица витрины, тип визуализации, настройки и место в сетке.
# Ширина задаётся в колонках сетки Superset, в строке их двенадцать.
CHARTS = [
    {
        "name": "Рейсов в витрине",
        "table": "dm_airline_punctuality",
        "width": 3, "height": 40,
        "params": {
            "viz_type": "big_number_total",
            "metric": metric("flights", "SUM", "рейсов"),
            "subheader": "всего рейсов за период наблюдения",
        },
    },
    {
        "name": "Средняя задержка вылета",
        "table": "dm_airline_punctuality",
        "width": 3, "height": 40,
        "params": {
            "viz_type": "big_number_total",
            # простое среднее по авиакомпаниям завышено: мелкий перевозчик
            # весит столько же, сколько крупный, поэтому взвешиваем по числу рейсов
            "metric": {"expressionType": "SQL", "label": "минут",
                       "sqlExpression": "sum(avg_delay_min * delays_known) "
                                        "/ sum(delays_known)"},
            "subheader": "на рейс, с учётом числа рейсов",
            "adhoc_filters": [where("direction", "==", "departures")],
        },
    },
    {
        "name": "Загрузка аэропорта по часам",
        "table": "dm_airport_load_hourly",
        "width": 6, "height": 40,
        "params": {
            "viz_type": "echarts_timeseries_bar",
            "x_axis": "hour_local",
            "metrics": [metric("flights_per_day", "SUM", "рейсов в день")],
            "groupby": ["direction"],
            "x_axis_sort_asc": True,
            "x_axis_title": "час по времени Кейптауна",
        },
    },
    {
        "name": "Пунктуальность авиакомпаний",
        "table": "dm_airline_punctuality",
        "width": 6, "height": 55,
        "params": {
            "viz_type": "echarts_timeseries_bar",
            "x_axis": "airline_name",
            "metrics": [metric("avg_delay_min", "AVG", "средняя задержка, мин")],
            "groupby": ["direction"],
            "adhoc_filters": [where("flights", ">=", 20)],
            "x_axis_label_rotation": 45,
        },
    },
    {
        "name": "Вклад источников: эксклюзивные наблюдения",
        "table": "dm_source_coverage",
        "width": 6, "height": 55,
        "params": {
            "viz_type": "pie",
            "groupby": ["source_name"],
            "metric": metric("exclusive_observations", "SUM", "наблюдений"),
            "donut": True, "show_labels": True, "label_type": "key_percent",
        },
    },
    {
        "name": "Откуда дует ветер в Кейптауне",
        "table": "dm_weather_runway",
        "width": 4, "height": 50,
        "params": {
            "viz_type": "pie",
            "groupby": ["wind_sector"],
            "metric": metric("observations", "SUM", "наблюдений"),
            "adhoc_filters": [where("icao_code", "==", "FACT")],
            "show_labels": True, "label_type": "key_percent",
        },
    },
    {
        "name": "Состав наблюдаемого парка",
        "table": "dm_fleet_mix",
        "width": 4, "height": 50,
        "params": {
            "viz_type": "treemap_v2",
            "groupby": ["type_desc"],
            "metric": metric("aircraft", "SUM", "бортов"),
            "row_limit": 15,
        },
    },
    {
        "name": "Трафик по дням",
        "table": "dm_traffic_daily",
        "width": 4, "height": 50,
        "params": {
            "viz_type": "echarts_timeseries_line",
            "x_axis": "date",
            "metrics": [metric("departures", "SUM", "вылетов"),
                        metric("arrivals", "SUM", "прилётов"),
                        metric("aircraft_seen", "SUM", "бортов в эфире")],
            "markerEnabled": True,
        },
    },
    {
        "name": "Задержки: среднее против медианы",
        "table": "dm_airline_punctuality",
        "width": 12, "height": 50,
        "params": {
            "viz_type": "table",
            "query_mode": "raw",
            "all_columns": ["airline_name", "direction", "flights", "avg_delay_min",
                            "median_delay_min", "p90_delay_min", "late_share",
                            "worst_delay_min"],
            "adhoc_filters": [where("flights", ">=", 20)],
            "order_by_cols": ['["avg_delay_min", false]'],
        },
    },
]


def api(method, path, token=None, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(BASE + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read().decode()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        detail = e.read().decode()[:400]
        raise RuntimeError(f"{method} {path} -> HTTP {e.code}: {detail}") from None


def login():
    answer = api("POST", "/api/v1/security/login", payload={
        "username": USER, "password": PASSWORD, "provider": "db", "refresh": True})
    return answer["access_token"]


def find(token, kind, field, value):
    query = urllib.parse.quote(json.dumps(
        {"filters": [{"col": field, "opr": "eq", "value": value}]}))
    found = api("GET", f"/api/v1/{kind}/?q={query}", token)["result"]
    return found[0]["id"] if found else None


def ensure_database(token):
    existing = find(token, "database", "database_name", DB_NAME)
    if existing:
        print(f"Источник уже есть: {DB_NAME}")
        return existing
    created = api("POST", "/api/v1/database/", token, {
        "database_name": DB_NAME, "sqlalchemy_uri": DB_URI,
        "expose_in_sqllab": True, "allow_dml": False})
    print(f"Создан источник: {DB_NAME}")
    return created["id"]


def ensure_datasets(token, database_id):
    datasets = {}
    for table in MARTS:
        existing = find(token, "dataset", "table_name", table)
        if existing:
            datasets[table] = existing
            continue
        created = api("POST", "/api/v1/dataset/", token, {
            "database": database_id, "schema": SCHEMA, "table_name": table})
        datasets[table] = created["id"]
        print(f"Создан набор данных: {table}")
    return datasets


def dataset_columns(token, dataset_id):
    return {c["column_name"] for c in api("GET", f"/api/v1/dataset/{dataset_id}", token)
            ["result"]["columns"]}


def check_columns(token, datasets):
    """Опечатка в имени столбца иначе всплывёт только при открытии дашборда."""
    for chart in CHARTS:
        known = dataset_columns(token, datasets[chart["table"]])
        used = set(chart["params"].get("groupby", []))
        used |= set(chart["params"].get("all_columns", []))
        if "x_axis" in chart["params"]:
            used.add(chart["params"]["x_axis"])
        for m in chart["params"].get("metrics", []) + [chart["params"].get("metric")]:
            if m and "column" in m:
                used.add(m["column"]["column_name"])
        for f in chart["params"].get("adhoc_filters", []):
            used.add(f["subject"])
        unknown = used - known
        if unknown:
            raise RuntimeError(f"{chart['name']}: нет таких столбцов — {', '.join(unknown)}")
    print(f"Столбцы всех {len(CHARTS)} графиков сверены с витринами")


def recreate_charts(token, datasets, dashboard_id):
    """График попадает на дашборд только через явную связь, одной разметки мало."""
    created = []
    for chart in CHARTS:
        existing = find(token, "chart", "slice_name", chart["name"])
        if existing:
            api("DELETE", f"/api/v1/chart/{existing}", token)
        dataset_id = datasets[chart["table"]]
        params = {"datasource": f"{dataset_id}__table", "time_range": "No filter",
                  "row_limit": 1000, "adhoc_filters": [], **chart["params"]}
        answer = api("POST", "/api/v1/chart/", token, {
            "slice_name": chart["name"], "viz_type": params["viz_type"],
            "datasource_id": dataset_id, "datasource_type": "table",
            "dashboards": [dashboard_id],
            "params": json.dumps(params, ensure_ascii=False)})
        created.append({**chart, "id": answer["id"]})
        print(f"График: {chart['name']}")
    return created


def verify(token, charts, datasets):
    """Прогоняет запрос каждого графика: опечатка в настройках иначе всплывёт на защите."""
    for chart in charts:
        params = chart["params"]
        columns = list(params.get("all_columns", []))
        if not columns:
            columns = ([params["x_axis"]] if "x_axis" in params else []) \
                      + list(params.get("groupby", []))
        metrics = params.get("metrics") or ([params["metric"]] if "metric" in params else [])
        query = {
            "columns": columns, "metrics": [] if params.get("query_mode") == "raw" else metrics,
            "filters": [{"col": f["subject"], "op": f["operator"], "val": f["comparator"]}
                        for f in params.get("adhoc_filters", [])],
            "extras": {"having": "", "where": ""}, "orderby": [], "annotation_layers": [],
            "row_limit": 1000, "series_limit": 0, "order_desc": True,
            "applied_time_extras": {}, "url_params": {}, "custom_params": {},
            "custom_form_data": {},
        }
        answer = api("POST", "/api/v1/chart/data", token, {
            "datasource": {"id": datasets[chart["table"]], "type": "table"},
            "queries": [query], "form_data": {"datasource": f"{datasets[chart['table']]}__table"},
            "result_format": "json", "result_type": "full"})
        rows = answer["result"][0].get("rowcount", 0)
        if not rows:
            raise RuntimeError(f"{chart['name']}: запрос вернул пусто")
        print(f"  {chart['name']}: {rows} строк")


def layout(charts):
    """Раскладка дашборда: строки по двенадцать колонок, графики по порядку."""
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASHBOARD}},
    }
    row, used = [], 0
    rows = []
    for chart in charts:
        if used + chart["width"] > 12:
            rows.append(row)
            row, used = [], 0
        row.append(chart)
        used += chart["width"]
    if row:
        rows.append(row)

    for r, items in enumerate(rows, start=1):
        row_id = f"ROW-{r}"
        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = {"type": "ROW", "id": row_id, "children": [],
                            "parents": ["ROOT_ID", "GRID_ID"],
                            "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for c, chart in enumerate(items, start=1):
            chart_id = f"CHART-{r}-{c}"
            position[row_id]["children"].append(chart_id)
            position[chart_id] = {
                "type": "CHART", "id": chart_id, "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {"chartId": chart["id"], "sliceName": chart["name"],
                         "width": chart["width"], "height": chart["height"]}}
    return position


def ensure_dashboard(token):
    """Дашборд переиспользуется, чтобы ссылка на него не менялась при каждом запуске."""
    existing = find(token, "dashboard", "dashboard_title", DASHBOARD)
    if existing:
        return existing
    created = api("POST", "/api/v1/dashboard/", token, {
        "dashboard_title": DASHBOARD, "published": True})
    return created["id"]


def place_charts(token, dashboard_id, charts):
    api("PUT", f"/api/v1/dashboard/{dashboard_id}", token, {
        "position_json": json.dumps(layout(charts), ensure_ascii=False)})


def main():
    token = login()
    database_id = ensure_database(token)
    datasets = ensure_datasets(token, database_id)
    check_columns(token, datasets)
    dashboard_id = ensure_dashboard(token)
    charts = recreate_charts(token, datasets, dashboard_id)
    print("Проверка запросов:")
    verify(token, charts, datasets)
    place_charts(token, dashboard_id, charts)
    print(f"\nДашборд готов: {BASE}/superset/dashboard/{dashboard_id}/")
    print(f"Графиков: {len(charts)}, вход admin / admin")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as e:
        print(f"Ошибка: {e}")
        sys.exit(1)
