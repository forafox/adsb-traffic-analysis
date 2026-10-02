#!/usr/bin/env python3
"""Первичный анализ собранных рейсов: загрузка аэропорта по часам и задержки по авиакомпаниям.

Считает прямо по снимкам табло из data/raw/board, запускается на хосте:

    python3 tools/analysis_charts.py

Складывает report/flights.png.
"""

import glob
import json
import os
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BOARD_DIR = os.path.join(ROOT, "data", "raw", "board")
OUT_PATH = os.path.join(ROOT, "report", "flights.png")

MIN_FLIGHTS = 15      # меньше рейсов — статистика по авиакомпании ненадёжна
MAX_DELAY = 240       # отсекаем разбор полуночи и явные ошибки табло
BLUE, ORANGE, INK, MUTED = "#2a78d6", "#eb6834", "#2b2b28", "#6f6f69"


def minutes(value):
    if not value or len(value) != 5 or value[2] != ":":
        return None
    try:
        return int(value[:2]) * 60 + int(value[3:])
    except ValueError:
        return None


def latest_flights():
    """Последнее состояние каждого рейса: табло снималось по нескольку раз за день."""
    latest = {}
    for path in sorted(glob.glob(os.path.join(BOARD_DIR, "*.json"))):
        with open(path, encoding="utf-8") as f:
            snapshot = json.load(f)
        for flight in snapshot["flights"]:
            key = (snapshot["date"], flight.get("direction"), flight.get("flight_number"),
                   flight.get("schedule_time"))
            latest[key] = {**flight, "date": snapshot["date"]}
    return list(latest.values())


def panel(ax, title, xlabel):
    ax.set_title(title, fontsize=11, color=INK, loc="left", pad=10)
    ax.set_xlabel(xlabel, fontsize=9, color=MUTED)
    ax.tick_params(labelsize=9, colors=MUTED, length=0)
    for spine in ("top", "right", "left"):
        ax.spines[spine].set_visible(False)
    ax.spines["bottom"].set_color("#d8d8d2")


def main():
    flights = latest_flights()
    departures = [f for f in flights if f.get("direction") == "departures"]

    per_hour = defaultdict(set)
    for f in departures:
        start = minutes(f.get("schedule_time"))
        if start is not None:
            per_hour[start // 60].add((f["date"], f.get("flight_number")))
    days = len({f["date"] for f in departures})
    hours = list(range(24))
    counts = [len(per_hour.get(h, ())) / days for h in hours]

    delays = defaultdict(list)
    for f in departures:
        planned, actual = minutes(f.get("schedule_time")), minutes(f.get("updated_time"))
        if planned is None or actual is None or f.get("status") not in ("Departed", "Landed"):
            continue
        if abs(actual - planned) < MAX_DELAY:
            delays[f.get("airline", "")].append(actual - planned)
    by_airline = sorted(((a, sum(d) / len(d), len(d)) for a, d in delays.items()
                         if len(d) >= MIN_FLIGHTS), key=lambda x: x[1])

    fig, (left, right) = plt.subplots(1, 2, figsize=(11, 3.4))
    fig.patch.set_facecolor("#fcfcfb")
    for ax in (left, right):
        ax.set_facecolor("#fcfcfb")

    left.bar(hours, counts, color=BLUE, width=0.7)
    panel(left, "Вылеты из Кейптауна по часам", "час по местному времени")
    left.set_ylabel("рейсов в день", fontsize=9, color=MUTED)
    left.set_xticks(range(0, 24, 3))

    names = [a.title() for a, _, _ in by_airline]
    right.barh(names, [d for _, d, _ in by_airline], color=ORANGE, height=0.6)
    panel(right, "Средняя задержка вылета", "минуты")
    right.set_xlim(0, max(d for _, d, _ in by_airline) * 1.25)
    for i, (_, delay, n) in enumerate(by_airline):
        right.text(delay * 1.03, i, f"{delay:.0f} ({n} рейсов)", va="center",
                   fontsize=9, color=INK)

    fig.tight_layout()
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    fig.savefig(OUT_PATH, dpi=160)
    print(f"Рейсов в выборке: {len(flights)}, дней: {days}")
    print("Записано", os.path.relpath(OUT_PATH, ROOT))


if __name__ == "__main__":
    main()
