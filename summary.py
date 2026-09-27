#!/usr/bin/env python3
"""Что уже собрано: список снимков из data/raw и сводка по источникам.

    python3 summary.py          таблица по запускам
    python3 summary.py --short  только итоги
"""

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone

SAST = timezone(timedelta(hours=2))  # время Кейптауна
ROOT = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(ROOT, "data", "raw")


def load_runs():
    if not os.path.isdir(RAW_DIR):
        return []
    runs = []
    for run_id in sorted(os.listdir(RAW_DIR)):
        path = os.path.join(RAW_DIR, run_id, "manifest.json")
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                runs.append(json.load(f))
    return runs


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--short", action="store_true", help="только итоги")
    args = p.parse_args()

    runs = load_runs()
    if not runs:
        print(f"Снимков пока нет. Запустите: python3 collect.py")
        return

    totals = defaultdict(lambda: {"runs": 0, "aircraft": 0, "bytes": 0, "errors": 0})
    if not args.short:
        print(f"{'запуск (UTC)':18s} {'Кейптаун':11s} {'источник':10s} {'регион':7s} "
              f"{'бортов':>7s} {'размер':>9s}")
    for run in runs:
        local = datetime.strptime(run["run_id"], "%Y%m%dT%H%M%SZ").replace(
            tzinfo=timezone.utc).astimezone(SAST).strftime("%d.%m %H:%M")
        for e in run["entries"]:
            t = totals[e["source"]]
            t["runs"] += 1
            if "error" in e:
                t["errors"] += 1
                cells = f"{'—':>7s} {e['error'][:40]:>9s}"
            else:
                t["aircraft"] += e["aircraft"] or 0
                t["bytes"] += e["bytes"]
                cells = f"{e['aircraft'] if e['aircraft'] is not None else '?':>7} " \
                        f"{e['bytes'] / 1024:8.1f}К"
            if not args.short:
                print(f"{run['run_id']:18s} {local:11s} {e['source']:10s} {e['region']:7s} {cells}")

    print(f"\nВсего снимков: {len(runs)}, с {runs[0]['run_id']} по {runs[-1]['run_id']}")
    print(f"{'источник':10s} {'запросов':>8s} {'ошибок':>7s} {'бортов всего':>13s} {'объём':>9s}")
    for source, t in sorted(totals.items()):
        print(f"{source:10s} {t['runs']:8d} {t['errors']:7d} {t['aircraft']:13d} "
              f"{t['bytes'] / 1024:8.1f}К")


if __name__ == "__main__":
    main()
