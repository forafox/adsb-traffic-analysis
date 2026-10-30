"""Загрузка сырых снимков в слой ODS.

Записи остаются сырыми: разобраны только поля, по которым потом идут запросы,
остальное лежит в колонке raw целиком.

Повторная загрузка того же снимка пропускается: проверяется наличие его run_id.
"""

import json
import os

from clickhouse import client

PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/project")
RAW_DIR = os.path.join(PROJECT_DIR, "data", "raw")
POSITIONS_DIR = os.path.join(RAW_DIR, "positions")
BOARD_DIR = os.path.join(RAW_DIR, "board")
WEATHER_DIR = os.path.join(RAW_DIR, "weather")

# колонки ODS: имя и тип
DATASETS = {
    "positions": [
        ("run_id", "String"), ("source", "String"), ("region", "String"),
        ("fetched_at", "String"), ("icao24", "String"), ("callsign", "String"),
        ("lat", "Nullable(Float64)"), ("lon", "Nullable(Float64)"),
        ("alt_m", "Nullable(Float64)"), ("on_ground", "UInt8"),
        ("speed_ms", "Nullable(Float64)"), ("track", "Nullable(Float64)"),
        ("raw", "String"),
    ],
    "flights": [
        ("run_id", "String"), ("flight_date", "String"), ("direction", "String"),
        ("flight_number", "String"), ("airline", "String"), ("schedule_time", "String"),
        ("updated_time", "String"), ("status", "String"), ("origin", "String"),
        ("destination", "String"), ("raw", "String"),
    ],
    "metar": [
        ("run_id", "String"), ("icao", "String"), ("report_time", "String"),
        ("wind_dir", "Nullable(Float64)"), ("wind_speed", "Nullable(Float64)"),
        ("visibility", "String"), ("temp_c", "Nullable(Float64)"),
        ("altimeter", "Nullable(Float64)"), ("raw_ob", "String"), ("raw", "String"),
    ],
}


def _num(value):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def read_positions(run_id):
    """Снимок позиций: три источника отдают разный формат, приводим к одному набору полей."""
    run_dir = os.path.join(POSITIONS_DIR, run_id)
    with open(os.path.join(run_dir, "manifest.json"), encoding="utf-8") as f:
        manifest = json.load(f)
    rows = []
    for entry in manifest["entries"]:
        if "error" in entry:
            continue
        with open(os.path.join(run_dir, entry["file"]), encoding="utf-8") as f:
            payload = json.load(f)
        common = {"run_id": run_id, "source": entry["source"], "region": entry["region"],
                  "fetched_at": entry["fetched_at"]}
        if entry["source"] == "opensky":
            for s in payload.get("states") or []:
                rows.append({**common, "icao24": s[0], "callsign": (s[1] or "").strip(),
                             "lat": _num(s[6]), "lon": _num(s[5]),
                             "alt_m": _num(s[7]) if s[7] is not None else _num(s[13]),
                             "on_ground": bool(s[8]), "speed_ms": _num(s[9]),
                             "track": _num(s[10]), "raw": json.dumps(s)})
        else:
            for a in payload.get("ac") or payload.get("aircraft") or []:
                alt = a.get("alt_baro")
                rows.append({**common, "icao24": (a.get("hex") or "").lstrip("~"),
                             "callsign": (a.get("flight") or "").strip(),
                             "lat": _num(a.get("lat")), "lon": _num(a.get("lon")),
                             "alt_m": round(alt * 0.3048, 1) if _num(alt) is not None else None,
                             "on_ground": alt == "ground",
                             "speed_ms": round(a["gs"] * 0.514444, 1) if _num(a.get("gs")) else None,
                             "track": _num(a.get("track")), "raw": json.dumps(a)})
    return rows


def read_flights(path):
    with open(path, encoding="utf-8") as f:
        snapshot = json.load(f)
    if any(not r.get("parsed") for r in snapshot["requests"]):
        print(f"Пропускаем неполный снимок табло: {os.path.basename(path)}")
        return []
    return [{"run_id": snapshot["run_id"], "flight_date": snapshot["date"],
             "direction": f.get("direction", ""), "flight_number": f.get("flight_number", ""),
             "airline": f.get("airline", ""), "schedule_time": f.get("schedule_time", ""),
             "updated_time": f.get("updated_time", ""), "status": f.get("status", ""),
             "origin": f.get("departing_from", ""), "destination": f.get("destination", ""),
             "raw": json.dumps(f, ensure_ascii=False)}
            for f in snapshot["flights"]]


def read_metar(path):
    with open(path, encoding="utf-8") as f:
        snapshot = json.load(f)
    return [{"run_id": snapshot["run_id"], "icao": o.get("icaoId", ""),
             "report_time": o.get("reportTime", ""), "wind_dir": _num(o.get("wdir")),
             "wind_speed": _num(o.get("wspd")), "visibility": str(o.get("visib", "")),
             "temp_c": _num(o.get("temp")), "altimeter": _num(o.get("altim")),
             "raw_ob": o.get("rawOb", ""), "raw": json.dumps(o, ensure_ascii=False)}
            for o in snapshot["observations"]]


def load(dataset, rows):
    """Вставляет снимок, если его ещё нет. Возвращает число загруженных строк."""
    if not rows:
        print("Нечего загружать")
        return 0

    columns = DATASETS[dataset]
    names = [name for name, _ in columns]
    table = f"ods_{dataset}"
    run_id = rows[0]["run_id"]

    ch = client()
    ch.command(f"create table if not exists {table} ("
               + ", ".join(f"{name} {type_}" for name, type_ in columns)
               + ", loaded_at DateTime default now()) engine = MergeTree order by run_id")
    if ch.query(f"select 1 from {table} where run_id = {{r:String}} limit 1",
                parameters={"r": run_id}).result_rows:
        print(f"{table}: снимок уже загружен (run_id {run_id})")
        return 0

    ch.insert(table, [[row.get(name) if row.get(name) is not None else
                       (0 if name == "on_ground" else None) for name in names] for row in rows],
              column_names=names)
    print(f"{table}: загружено {len(rows)} строк (run_id {run_id})")
    return len(rows)
