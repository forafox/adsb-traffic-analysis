#!/usr/bin/env python3
"""Ручной сбор снимка воздушного движения над Кейптауном.

Один запуск = один снимок: скрипт опрашивает источники и складывает ответы
как есть в отдельную папку с меткой времени UTC:

    data/raw/20260927T101500Z/
        manifest.json      что, откуда и когда забрали
        opensky_cpt.json   ответ OpenSky без изменений
        adsblol_cpt.json   ответ adsb.lol без изменений

Ответы не парсятся и не переписываются: в хранилище ЛР1 они пойдут сырыми.

Запуск:
    python3 collect.py                          один снимок
    python3 collect.py --repeat 12              серия из 12 снимков с паузой 5 минут
    python3 collect.py --repeat 12 --interval 120   та же серия, но раз в 2 минуты
    python3 collect.py --regions cpt            только Кейптаун, без остальной ЮАР
    python3 collect.py --sources opensky        только один источник
    python3 collect.py --dry-run                показать, что будет собрано, без записи

Серия прерывается по Ctrl+C, уже сохранённые снимки при этом остаются.

Зависимостей нет, нужен только Python 3.9+.
"""

import argparse
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "adsb-traffic-analysis/1.0 (university data engineering project)"
TIMEOUT = 30

# bbox: (lat_min, lon_min, lat_max, lon_max)
REGIONS = {
    "cpt": (-34.5, 17.9, -33.4, 19.3),   # Кейптаун и аэропорт CPT, ~120x130 км
    "za": (-35.0, 16.0, -22.0, 33.0),    # вся ЮАР, для оценки объёма данных
}
DEFAULT_REGIONS = "cpt,za"
DEFAULT_SOURCES = "opensky,adsblol,adsbfi"
DEFAULT_INTERVAL = 300

ROOT = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(ROOT, "data", "raw")


def ssl_context():
    """Python с python.org на macOS не видит системные сертификаты, подставляем их явно."""
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        pass
    if os.path.exists("/etc/ssl/cert.pem"):
        return ssl.create_default_context(cafile="/etc/ssl/cert.pem")
    return ssl.create_default_context()


SSL_CTX = ssl_context()


def opensky_url(bbox):
    lat0, lon0, lat1, lon1 = bbox
    return ("https://opensky-network.org/api/states/all"
            f"?lamin={lat0}&lomin={lon0}&lamax={lat1}&lomax={lon1}&extended=1")


def adsblol_url(bbox):
    lat0, lon0, lat1, lon1 = bbox
    clat, clon = (lat0 + lat1) / 2, (lon0 + lon1) / 2
    radius = min(250, int(max(lat1 - lat0, lon1 - lon0) * 60 / 2) + 1)  # в морских милях
    return f"https://api.adsb.lol/v2/point/{clat:.4f}/{clon:.4f}/{radius}"


def adsbfi_url(bbox):
    lat0, lon0, lat1, lon1 = bbox
    clat, clon = (lat0 + lat1) / 2, (lon0 + lon1) / 2
    radius = min(250, int(max(lat1 - lat0, lon1 - lon0) * 60 / 2) + 1)
    return f"https://opendata.adsb.fi/api/v2/lat/{clat:.4f}/lon/{clon:.4f}/dist/{radius}"


SOURCES = {
    "opensky": opensky_url,
    "adsblol": adsblol_url,
    "adsbfi": adsbfi_url,
}


def count_aircraft(body):
    """Сколько бортов в ответе. Нужно только для лога, сам ответ не меняется."""
    try:
        data = json.loads(body)
    except ValueError:
        return None
    for key in ("states", "ac", "aircraft"):
        if isinstance(data.get(key), list):
            return len(data[key])
    return None


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=TIMEOUT, context=SSL_CTX) as resp:
        return resp.status, resp.read().decode("utf-8")


def collect(regions, sources, dry_run=False):
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = os.path.join(RAW_DIR, run_id)
    print(f"Снимок {run_id}" + (" (пробный прогон)" if dry_run else ""))

    entries = []
    failed = 0
    for region in regions:
        for source in sources:
            url = SOURCES[source](REGIONS[region])
            name = f"{source}_{region}.json"
            if dry_run:
                print(f"  {name:22s} {url}")
                continue
            started = time.monotonic()
            entry = {"source": source, "region": region, "bbox": list(REGIONS[region]),
                     "url": url, "file": name}
            try:
                status, body = fetch(url)
            except urllib.error.HTTPError as e:
                entry["error"] = f"HTTP {e.code}"
            except Exception as e:
                entry["error"] = f"{type(e).__name__}: {e}"[:200]
            else:
                entry.update(http_status=status, bytes=len(body.encode("utf-8")),
                             aircraft=count_aircraft(body))
                os.makedirs(run_dir, exist_ok=True)
                with open(os.path.join(run_dir, name), "w", encoding="utf-8") as f:
                    f.write(body)
            entry["duration_s"] = round(time.monotonic() - started, 2)
            entry["fetched_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            entries.append(entry)

            if "error" in entry:
                failed += 1
                print(f"  {name:22s} ошибка: {entry['error']}")
            else:
                n = entry["aircraft"]
                print(f"  {name:22s} {n if n is not None else '?'} бортов, "
                      f"{entry['bytes'] / 1024:.1f} КБ")
            time.sleep(1)  # adsb.fi просит не чаще одного запроса в секунду

    if dry_run:
        return run_id, 0

    os.makedirs(run_dir, exist_ok=True)
    manifest = {"run_id": run_id, "regions": regions, "sources": sources, "entries": entries}
    with open(os.path.join(run_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"Сохранено в data/raw/{run_id}, файлов: {len(entries) - failed}")
    return run_id, failed


def collect_series(regions, sources, repeat, interval, dry_run=False):
    if dry_run:
        collect(regions, sources, dry_run=True)
        print(f"Серия: {repeat} снимков с паузой {interval} с")
        return 0
    failed = 0
    try:
        for i in range(repeat):
            print(f"[{i + 1}/{repeat}] ", end="")
            failed += collect(regions, sources)[1]
            if i + 1 < repeat:
                time.sleep(interval)
    except KeyboardInterrupt:
        print("\nОстановлено, собранные снимки сохранены.")
    if failed:
        print(f"Запросов с ошибкой: {failed}. Если их много, проверьте лимиты источников.")
    return 1 if failed else 0


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--regions", default=DEFAULT_REGIONS,
                   help=f"регионы через запятую из: {', '.join(REGIONS)} (по умолчанию {DEFAULT_REGIONS})")
    p.add_argument("--sources", default=DEFAULT_SOURCES,
                   help=f"источники через запятую из: {', '.join(SOURCES)} (по умолчанию {DEFAULT_SOURCES})")
    p.add_argument("--repeat", type=int, default=1, metavar="N",
                   help="сколько снимков сделать за запуск (по умолчанию 1)")
    p.add_argument("--interval", type=int, default=DEFAULT_INTERVAL, metavar="СЕК",
                   help=f"пауза между снимками в серии (по умолчанию {DEFAULT_INTERVAL})")
    p.add_argument("--dry-run", action="store_true", help="показать запросы и выйти")
    args = p.parse_args()

    regions = [r.strip() for r in args.regions.split(",") if r.strip()]
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    for name, chosen, known in (("регионы", regions, REGIONS), ("источники", sources, SOURCES)):
        unknown = [x for x in chosen if x not in known]
        if unknown:
            p.error(f"неизвестные {name}: {', '.join(unknown)}; доступны: {', '.join(known)}")
    if args.repeat < 1:
        p.error("--repeat должен быть не меньше 1")
    sys.exit(collect_series(regions, sources, args.repeat, args.interval, args.dry_run))


if __name__ == "__main__":
    main()
