"""Загрузка сырых снимков в слой ODS трёх хранилищ.

Один и тот же набор записей пишется в Postgres, ClickHouse и MongoDB, чтобы хранилища
можно было сравнивать на одинаковых данных. Записи остаются сырыми: разобраны только
поля, по которым потом идут запросы, остальное лежит в колонке raw целиком.

Повторная загрузка того же снимка пропускается: проверяется наличие его run_id.
"""

import json
import os

PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/project")
RAW_DIR = os.path.join(PROJECT_DIR, "data", "raw")

PG_DSN = os.environ["ODS_POSTGRES_DSN"]
CH_HOST = os.environ["ODS_CLICKHOUSE_HOST"]
MONGO_URI = os.environ["ODS_MONGO_URI"]

# колонки ODS: имя, тип в Postgres, тип в ClickHouse
DATASETS = {
    "positions": [
        ("run_id", "text", "String"), ("source", "text", "String"),
        ("region", "text", "String"), ("fetched_at", "text", "String"),
        ("icao24", "text", "String"), ("callsign", "text", "String"),
        ("lat", "double precision", "Nullable(Float64)"),
        ("lon", "double precision", "Nullable(Float64)"),
        ("alt_m", "double precision", "Nullable(Float64)"),
        ("on_ground", "boolean", "UInt8"),
        ("speed_ms", "double precision", "Nullable(Float64)"),
        ("track", "double precision", "Nullable(Float64)"),
        ("raw", "jsonb", "String"),
    ],
    "flights": [
        ("run_id", "text", "String"), ("flight_date", "text", "String"),
        ("direction", "text", "String"), ("flight_number", "text", "String"),
        ("airline", "text", "String"), ("schedule_time", "text", "String"),
        ("updated_time", "text", "String"), ("status", "text", "String"),
        ("origin", "text", "String"), ("destination", "text", "String"),
        ("raw", "jsonb", "String"),
    ],
    "metar": [
        ("run_id", "text", "String"), ("icao", "text", "String"),
        ("report_time", "text", "String"),
        ("wind_dir", "double precision", "Nullable(Float64)"),
        ("wind_speed", "double precision", "Nullable(Float64)"),
        ("visibility", "text", "String"),
        ("temp_c", "double precision", "Nullable(Float64)"),
        ("altimeter", "double precision", "Nullable(Float64)"),
        ("raw_ob", "text", "String"), ("raw", "jsonb", "String"),
    ],
}


def _num(value):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def read_positions(run_id):
    """Снимок позиций: три источника отдают разный формат, приводим к одному набору полей."""
    run_dir = os.path.join(RAW_DIR, run_id)
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


def load_postgres(dataset, rows):
    import psycopg2
    from psycopg2.extras import execute_values

    columns = DATASETS[dataset]
    names = [c[0] for c in columns]
    table = f"ods_{dataset}"
    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute(f"create table if not exists {table} ("
                    + ", ".join(f"{n} {t}" for n, t, _ in columns)
                    + ", loaded_at timestamptz default now())")
        cur.execute(f"create index if not exists {table}_run_id on {table} (run_id)")
        cur.execute(f"select 1 from {table} where run_id = %s limit 1", (rows[0]["run_id"],))
        if cur.fetchone():
            return 0
        execute_values(cur, f"insert into {table} ({', '.join(names)}) values %s",
                       [tuple(r.get(n) for n in names) for r in rows])
    return len(rows)


def load_clickhouse(dataset, rows):
    import clickhouse_connect

    columns = DATASETS[dataset]
    names = [c[0] for c in columns]
    table = f"ods_{dataset}"
    client = clickhouse_connect.get_client(host=CH_HOST, username="ods", password="ods",
                                           database="ods")
    client.command(f"create table if not exists {table} ("
                   + ", ".join(f"{n} {t}" for n, _, t in columns)
                   + ", loaded_at DateTime default now()) engine = MergeTree order by run_id")
    if client.query(f"select 1 from {table} where run_id = {{r:String}} limit 1",
                    parameters={"r": rows[0]["run_id"]}).result_rows:
        return 0
    client.insert(table, [[r.get(n) if r.get(n) is not None else
                           (0 if n == "on_ground" else None) for n in names] for r in rows],
                  column_names=names)
    return len(rows)


def load_mongo(dataset, rows):
    from pymongo import MongoClient

    collection = MongoClient(MONGO_URI)["ods"][f"ods_{dataset}"]
    collection.create_index("run_id")
    if collection.find_one({"run_id": rows[0]["run_id"]}):
        return 0
    # в документной базе raw хранится объектом, а не строкой
    collection.insert_many([{**r, "raw": json.loads(r["raw"])} for r in rows])
    return len(rows)


LOADERS = {"postgres": load_postgres, "clickhouse": load_clickhouse, "mongo": load_mongo}


def load(dataset, storage, rows):
    if not rows:
        print("Нечего загружать")
        return 0
    inserted = LOADERS[storage](dataset, rows)
    print(f"{storage}: {'загружено ' + str(inserted) if inserted else 'снимок уже загружен'}"
          f" ({dataset}, run_id {rows[0]['run_id']})")
    return inserted
