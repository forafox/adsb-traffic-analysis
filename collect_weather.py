#!/usr/bin/env python3
"""Сбор сводок погоды METAR по аэропортам ЮАР с aviationweather.gov.

METAR нужен для анализа: выбор посадочной полосы зависит от ветра, а время захода
на посадку от видимости и облачности.

Сводки выходят раз в час, поэтому запрос берёт сразу историю за последние часы.
Даже если сбор запускать дважды в день, пропусков не будет.

Запуск:
    python3 collect_weather.py                  последние 12 часов по четырём аэропортам
    python3 collect_weather.py --hours 24       за сутки
    python3 collect_weather.py --airports FACT  только Кейптаун
    python3 collect_weather.py --dry-run        показать запрос, ничего не сохраняя

Ключ доступа не нужен.
"""

import argparse
import json
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

API = "https://aviationweather.gov/api/data/metar"
USER_AGENT = "adsb-traffic-analysis/1.0 (university data engineering project)"
TIMEOUT = 40

# FACT Кейптаун, FAOR Йоханнесбург, FALE Дурбан, FAPE Гкеберха
DEFAULT_AIRPORTS = "FACT,FAOR,FALE,FAPE"
DEFAULT_HOURS = 12

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(ROOT, "data", "raw", "weather")


def ssl_context():
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        pass
    if os.path.exists("/etc/ssl/cert.pem"):
        return ssl.create_default_context(cafile="/etc/ssl/cert.pem")
    return ssl.create_default_context()


def collect(airports, hours, dry_run=False):
    url = f"{API}?" + urllib.parse.urlencode({"ids": ",".join(airports),
                                              "format": "json", "hours": hours})
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"Погода {run_id}: {', '.join(airports)}, последние {hours} ч")
    if dry_run:
        print(f"  GET {url}")
        return 0

    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=TIMEOUT, context=ssl_context()) as resp:
            body = resp.read().decode("utf-8")
        observations = json.loads(body)
    except urllib.error.HTTPError as e:
        print(f"  ошибка: HTTP {e.code}")
        return 1
    except Exception as e:
        print(f"  ошибка: {type(e).__name__}: {e}"[:200])
        return 1

    by_airport = {}
    for o in observations:
        by_airport[o.get("icaoId")] = by_airport.get(o.get("icaoId"), 0) + 1

    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f"{run_id}_metar.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"run_id": run_id, "source": "aviationweather", "url": url,
                   "airports": airports, "hours": hours,
                   "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                   "observations": observations}, f, ensure_ascii=False, indent=2)

    print("  " + ", ".join(f"{a}: {n}" for a, n in sorted(by_airport.items())))
    print(f"Сохранено в data/raw/weather/{os.path.basename(path)}, "
          f"наблюдений: {len(observations)}")
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--airports", default=DEFAULT_AIRPORTS,
                   help=f"коды ICAO через запятую (по умолчанию {DEFAULT_AIRPORTS})")
    p.add_argument("--hours", type=int, default=DEFAULT_HOURS,
                   help=f"за сколько последних часов брать сводки (по умолчанию {DEFAULT_HOURS})")
    p.add_argument("--dry-run", action="store_true", help="показать запрос и выйти")
    args = p.parse_args()
    airports = [a.strip().upper() for a in args.airports.split(",") if a.strip()]
    if not airports:
        p.error("нужен хотя бы один код аэропорта")
    if not 1 <= args.hours <= 96:
        p.error("--hours должен быть от 1 до 96")
    sys.exit(collect(airports, args.hours, args.dry_run))


if __name__ == "__main__":
    main()
