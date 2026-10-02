#!/usr/bin/env python3
"""Замеры по трём хранилищам ODS: вставка, объём на диске, типовые запросы.

Запускается внутри контейнера Airflow, где уже стоят драйверы:

    docker compose exec airflow-scheduler python /opt/project/tools/compare_storages.py

Замеры идут на отдельной таблице bench_flights. В неё собранные рейсы копируются
SCALE раз, каждой копии выдаётся свой run_id: на реально собранных 3,5 тысячах записей
разница между хранилищами тонет в накладных расходах на один запрос.

Каждому хранилищу даётся его обычная оптимизация по run_id: индекс в Postgres,
ключ сортировки в ClickHouse, индекс в MongoDB.

Результат печатается таблицей и складывается в data/compare/metrics.json,
оттуда его берёт tools/compare_charts.py.
"""

import glob
import json
import os
import statistics
import sys
import time

sys.path.insert(0, "/opt/airflow/dags")

import ods

SCALE = 60
REPEATS = 5
TABLE = "bench_flights"
COLUMNS = ods.DATASETS["flights"]
NAMES = [c[0] for c in COLUMNS]
OUT_PATH = os.path.join(ods.PROJECT_DIR, "data", "compare", "metrics.json")


def bench_rows():
    """Собранные рейсы, размноженные SCALE раз: у каждой копии свой run_id."""
    source = [r for path in sorted(glob.glob(os.path.join(ods.RAW_DIR, "board", "*.json")))
              for r in ods.read_flights(path)]
    rows = [{**r, "run_id": f"{r['run_id']}-c{copy}"}
            for copy in range(SCALE) for r in source]
    return rows, f"{source[0]['run_id']}-c0"


def timed(fn, repeats=REPEATS):
    """Минимум из нескольких прогонов: меньше влияния случайных задержек."""
    times = []
    for _ in range(repeats):
        started = time.perf_counter()
        fn()
        times.append(time.perf_counter() - started)
    return round(min(times) * 1000, 1), round(statistics.median(times) * 1000, 1)


def postgres_bench(rows, run_id):
    import psycopg2
    from psycopg2.extras import execute_values

    values = [tuple(r.get(n) for n in NAMES) for r in rows]
    conn = psycopg2.connect(ods.PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()

    def insert():
        cur.execute(f"drop table if exists {TABLE}")
        cur.execute(f"create table {TABLE} ("
                    + ", ".join(f"{n} {t}" for n, t, _ in COLUMNS) + ")")
        execute_values(cur, f"insert into {TABLE} ({', '.join(NAMES)}) values %s", values,
                       page_size=5000)
        cur.execute(f"create index on {TABLE} (run_id)")
        cur.execute(f"analyze {TABLE}")

    def query(sql, params=None):
        def run():
            cur.execute(sql, params)
            cur.fetchall()
        return run

    result = {
        "insert": timed(insert, 1),
        "point": timed(query(f"select count(*) from {TABLE} where run_id = %s", (run_id,))),
        "group": timed(query(f"select airline, count(*) from {TABLE} group by airline")),
        "nested": timed(query(f"select count(*) from {TABLE} where raw->>'gate' <> ''")),
    }
    cur.execute(f"select pg_total_relation_size('{TABLE}')")
    result["bytes"] = int(cur.fetchone()[0])
    cur.execute("select coalesce(sum(pg_total_relation_size(c.oid)), 0) from pg_class c "
                "join pg_namespace n on n.oid = c.relnamespace "
                "where c.relname like 'ods\\_%' and n.nspname = 'public'")
    result["ods_bytes"] = int(cur.fetchone()[0])
    cur.execute(f"drop table if exists {TABLE}")
    conn.close()
    return result


def clickhouse_bench(rows, run_id):
    import clickhouse_connect

    values = [[r.get(n) for n in NAMES] for r in rows]
    client = clickhouse_connect.get_client(host=ods.CH_HOST, username="ods", password="ods",
                                           database="ods")

    def insert():
        client.command(f"drop table if exists {TABLE}")
        client.command(f"create table {TABLE} ("
                       + ", ".join(f"{n} {t}" for n, _, t in COLUMNS)
                       + ") engine = MergeTree order by run_id")
        client.insert(TABLE, values, column_names=NAMES)

    def query(sql, params=None):
        return lambda: client.query(sql, parameters=params or {}).result_rows

    result = {
        "insert": timed(insert, 1),
        "point": timed(query(f"select count() from {TABLE} where run_id = {{r:String}}",
                             {"r": run_id})),
        "group": timed(query(f"select airline, count() from {TABLE} group by airline")),
        "nested": timed(query(f"select count() from {TABLE} "
                              "where JSONExtractString(raw, 'gate') != ''")),
    }
    size = lambda where: int(client.query(
        "select coalesce(sum(bytes_on_disk), 0) from system.parts "
        f"where database = 'ods' and active and {where}").result_rows[0][0])
    result["bytes"] = size(f"table = '{TABLE}'")
    result["ods_bytes"] = size("table like 'ods\\_%'")
    client.command(f"drop table if exists {TABLE}")
    return result


def mongo_bench(rows, run_id):
    from pymongo import MongoClient

    db = MongoClient(ods.MONGO_URI)["ods"]
    docs = [{**r, "raw": json.loads(r["raw"])} for r in rows]

    def insert():
        db[TABLE].drop()
        db[TABLE].insert_many([dict(d) for d in docs])
        db[TABLE].create_index("run_id")

    result = {
        "insert": timed(insert, 1),
        "point": timed(lambda: db[TABLE].count_documents({"run_id": run_id})),
        "group": timed(lambda: list(db[TABLE].aggregate(
            [{"$group": {"_id": "$airline", "n": {"$sum": 1}}}]))),
        "nested": timed(lambda: db[TABLE].count_documents({"raw.gate": {"$nin": ["", None]}})),
    }
    size = lambda names: sum(db.command("collstats", n)["storageSize"]
                             + db.command("collstats", n).get("totalIndexSize", 0) for n in names)
    result["bytes"] = size([TABLE])
    result["ods_bytes"] = size([n for n in db.list_collection_names() if n.startswith("ods_")])
    db[TABLE].drop()
    return result


BENCHES = {"postgres": postgres_bench, "clickhouse": clickhouse_bench, "mongo": mongo_bench}
QUERIES = {"point": "точечный по run_id", "group": "агрегация по авиакомпаниям",
           "nested": "поиск по вложенному полю raw"}


def main():
    rows, run_id = bench_rows()
    print(f"Набор для замеров: {len(rows)} записей ({SCALE} копий собранных рейсов)\n")

    metrics = {}
    for name, bench in BENCHES.items():
        print(f"Замеряю {name}")
        metrics[name] = bench(rows, run_id)
    metrics["_meta"] = {"rows": len(rows), "scale": SCALE, "repeats": REPEATS}

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    names = list(BENCHES)
    print(f"\n{'показатель':36s}" + "".join(f"{n:>13s}" for n in names))
    print(f"{'вставка ' + str(len(rows)) + ' записей, с':36s}"
          + "".join(f"{metrics[n]['insert'][0] / 1000:13.1f}" for n in names))
    print(f"{'объём набора на диске, МБ':36s}"
          + "".join(f"{metrics[n]['bytes'] / 1024 / 1024:13.1f}" for n in names))
    print(f"{'объём слоя ODS на диске, МБ':36s}"
          + "".join(f"{metrics[n]['ods_bytes'] / 1024 / 1024:13.1f}" for n in names))
    for key, title in QUERIES.items():
        print(f"{title + ', мс':36s}" + "".join(f"{metrics[n][key][0]:13.1f}" for n in names))
    print(f"\nЗаписано в {OUT_PATH}")


if __name__ == "__main__":
    main()
