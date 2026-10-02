#!/usr/bin/env python3
"""Графики по замерам хранилищ из data/compare/metrics.json.

Запускается на хосте, нужен matplotlib:

    python3 tools/compare_charts.py

Складывает report/storage.png и report/queries.png.
"""

import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
METRICS = os.path.join(ROOT, "data", "compare", "metrics.json")
REPORT_DIR = os.path.join(ROOT, "report")

STORAGES = ["postgres", "clickhouse", "mongo"]
COLORS = {"postgres": "#2a78d6", "clickhouse": "#eb6834", "mongo": "#1baf7a"}
INK = "#2b2b28"
MUTED = "#6f6f69"


def panel(ax, values, title, unit, fmt="{:.1f}"):
    bars = ax.barh(STORAGES[::-1], [values[s] for s in STORAGES[::-1]], height=0.6,
                   color=[COLORS[s] for s in STORAGES[::-1]])
    ax.set_title(title, fontsize=11, color=INK, loc="left", pad=10)
    ax.set_xlabel(unit, fontsize=9, color=MUTED)
    ax.set_xlim(0, max(values.values()) * 1.25)
    ax.tick_params(labelsize=9, colors=MUTED, length=0)
    for spine in ("top", "right", "left"):
        ax.spines[spine].set_visible(False)
    ax.spines["bottom"].set_color("#d8d8d2")
    for bar, storage in zip(bars, STORAGES[::-1]):
        ax.text(bar.get_width() * 1.03, bar.get_y() + bar.get_height() / 2,
                fmt.format(values[storage]), va="center", fontsize=9, color=INK)


def figure(path, panels, suptitle):
    fig, axes = plt.subplots(1, len(panels), figsize=(4.2 * len(panels), 2.9))
    fig.patch.set_facecolor("#fcfcfb")
    for ax, (values, title, unit) in zip(axes, panels):
        ax.set_facecolor("#fcfcfb")
        panel(ax, values, title, unit)
    fig.suptitle(suptitle, fontsize=12, color=INK, x=0.02, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(path, dpi=160)
    print("Записано", os.path.relpath(path, ROOT))


def main():
    if not os.path.exists(METRICS):
        sys.exit(f"Нет замеров: {METRICS}. Сначала запустите tools/compare_storages.py")
    with open(METRICS, encoding="utf-8") as f:
        m = json.load(f)
    rows = m["_meta"]["rows"]
    os.makedirs(REPORT_DIR, exist_ok=True)

    figure(os.path.join(REPORT_DIR, "storage.png"), [
        ({s: m[s]["insert"][0] / 1000 for s in STORAGES}, "Вставка набора", "секунды"),
        ({s: m[s]["bytes"] / 1024 / 1024 for s in STORAGES}, "Объём набора на диске", "МБ"),
    ], f"Загрузка и хранение {rows} записей")

    figure(os.path.join(REPORT_DIR, "queries.png"), [
        ({s: m[s]["point"][0] for s in STORAGES}, "Точечный по run_id", "мс"),
        ({s: m[s]["group"][0] for s in STORAGES}, "Агрегация по авиакомпаниям", "мс"),
        ({s: m[s]["nested"][0] for s in STORAGES}, "Поиск по вложенному полю", "мс"),
    ], f"Запросы к набору из {rows} записей, лучшее из {m['_meta']['repeats']} прогонов")


if __name__ == "__main__":
    main()
