#!/usr/bin/env python3
"""Проверка покрытия ADS-B по регионам ЮАР (по умолчанию Кейптаун и Йоханнесбург).

Периодически опрашивает OpenSky Network, adsb.lol и adsb.fi, пишет:
  coverage_data/coverage.csv  — одна строка на (опрос, регион, источник): сколько бортов видно
  coverage_data/aircraft.csv  — сами борты: позывной, координаты, высота, качество позиции

Только стандартная библиотека Python, внешние зависимости не нужны.

Использование:
  python3 coverage_check.py                 # опрос каждые 10 минут, Ctrl+C для остановки
  python3 coverage_check.py --once          # один опрос и выход
  python3 coverage_check.py --interval 300  # свой интервал в секундах
  python3 coverage_check.py --regions cpt,za   # другие регионы (список в REGIONS)
  python3 coverage_check.py --report        # сводка по накопленным данным

Лимит OpenSky без регистрации — 400 запросов в сутки: при интервале 10 минут это
не больше двух регионов (2 x 144 = 288). Больше регионов — увеличьте интервал.

Для OpenSky с учётной записью (больше лимит) задайте переменные окружения
OPENSKY_CLIENT_ID и OPENSKY_CLIENT_SECRET (API client из личного кабинета OpenSky).
"""

import argparse
import csv
import json
import math
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

MSK = timezone(timedelta(hours=3))
USER_AGENT = "itmo-data-lab-coverage-check/1.0"
TIMEOUT = 30
FRESH_SEC = 60  # позиция старше этого считается «устаревшей»

# bbox: (lat_min, lon_min, lat_max, lon_max)
REGIONS = {
    "cpt": (-34.5, 17.9, -33.4, 19.3),     # Кейптаун (CPT), ~120x130 км
    "jnb": (-26.7, 27.6, -25.6, 28.9),     # Йоханнесбург (JNB) + Претория
    "za": (-35.0, 16.0, -22.0, 33.0),      # вся ЮАР
}
DEFAULT_REGIONS = "cpt,jnb"

OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "data", "coverage")
COVERAGE_CSV = os.path.join(OUT_DIR, "coverage.csv")
AIRCRAFT_CSV = os.path.join(OUT_DIR, "aircraft.csv")

COVERAGE_FIELDS = ["ts_utc", "region", "source", "n_total", "n_airborne",
                   "n_fresh", "n_gps_degraded", "latency_s", "error"]
AIRCRAFT_FIELDS = ["ts_utc", "region", "source", "icao24", "callsign", "registration",
                   "type", "lat", "lon", "alt_m", "on_ground", "speed_ms", "track",
                   "vrate_ms", "squawk", "pos_age_s", "pos_source", "nac_p", "nic"]

FT = 0.3048
KT = 0.514444
FPM = 0.00508


def make_ssl_context():
    """Python с python.org на macOS не видит системные сертификаты — подставляем их явно."""
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        pass
    if os.path.exists("/etc/ssl/cert.pem"):
        return ssl.create_default_context(cafile="/etc/ssl/cert.pem")
    return ssl.create_default_context()


SSL_CTX = make_ssl_context()


def http_get_json(url, headers=None):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, **(headers or {})})
    with urllib.request.urlopen(req, timeout=TIMEOUT, context=SSL_CTX) as resp:
        return json.load(resp)


def in_bbox(lat, lon, bbox):
    return lat is not None and lon is not None and bbox[0] <= lat <= bbox[2] and bbox[1] <= lon <= bbox[3]


# ---------------------------------------------------------------- OpenSky

_opensky_token = {"value": None, "expires": 0.0}


def opensky_auth_header():
    cid, secret = os.environ.get("OPENSKY_CLIENT_ID"), os.environ.get("OPENSKY_CLIENT_SECRET")
    if not (cid and secret):
        return {}
    if time.time() > _opensky_token["expires"] - 60:
        body = urllib.parse.urlencode({"grant_type": "client_credentials",
                                       "client_id": cid, "client_secret": secret}).encode()
        req = urllib.request.Request(
            "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token",
            data=body, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=TIMEOUT, context=SSL_CTX) as resp:
            tok = json.load(resp)
        _opensky_token["value"] = tok["access_token"]
        _opensky_token["expires"] = time.time() + tok.get("expires_in", 1800)
    return {"Authorization": f"Bearer {_opensky_token['value']}"}


def fetch_opensky(bbox):
    lat0, lon0, lat1, lon1 = bbox
    url = ("https://opensky-network.org/api/states/all?"
           f"lamin={lat0}&lomin={lon0}&lamax={lat1}&lomax={lon1}&extended=1")
    data = http_get_json(url, opensky_auth_header())
    now = data.get("time") or time.time()
    pos_sources = {0: "adsb", 1: "asterix", 2: "mlat", 3: "flarm"}
    rows = []
    for s in data.get("states") or []:
        # https://openskynetwork.github.io/opensky-api/rest.html#response
        time_pos, lon, lat, baro_alt, on_ground = s[3], s[5], s[6], s[7], s[8]
        geo_alt = s[13]
        rows.append({
            "icao24": s[0], "callsign": (s[1] or "").strip(), "registration": "", "type": "",
            "lat": lat, "lon": lon,
            "alt_m": baro_alt if baro_alt is not None else geo_alt,
            "on_ground": bool(on_ground), "speed_ms": s[9], "track": s[10], "vrate_ms": s[11],
            "squawk": s[14] or "",
            "pos_age_s": round(now - time_pos, 1) if time_pos else None,
            "pos_source": pos_sources.get(s[16], str(s[16])), "nac_p": None, "nic": None,
        })
    return rows


# ---------------------------------------------------------------- adsb.lol / adsb.fi

def bbox_to_circle(bbox):
    """Центр bbox и радиус (в морских милях), покрывающий его целиком."""
    lat0, lon0, lat1, lon1 = bbox
    clat, clon = (lat0 + lat1) / 2, (lon0 + lon1) / 2
    dlat_nm = (lat1 - lat0) / 2 * 60
    dlon_nm = (lon1 - lon0) / 2 * 60 * math.cos(math.radians(clat))
    return clat, clon, min(250, math.ceil(math.hypot(dlat_nm, dlon_nm)))


def fetch_adsblol(bbox):
    clat, clon, radius = bbox_to_circle(bbox)
    return parse_readsb(http_get_json(f"https://api.adsb.lol/v2/point/{clat:.4f}/{clon:.4f}/{radius}"), bbox)


def fetch_adsbfi(bbox):
    clat, clon, radius = bbox_to_circle(bbox)
    url = f"https://opendata.adsb.fi/api/v2/lat/{clat:.4f}/lon/{clon:.4f}/dist/{radius}"
    return parse_readsb(http_get_json(url), bbox)


def parse_readsb(data, bbox):
    """adsb.lol и adsb.fi отдают один и тот же формат readsb."""
    rows = []
    for a in data.get("ac") or data.get("aircraft") or []:  # adsb.lol: "ac", adsb.fi: "aircraft"
        lat, lon = a.get("lat"), a.get("lon")
        if lat is None:  # позиция старая, но есть в rr_lat/rr_lon — грубая оценка
            lat, lon = a.get("rr_lat"), a.get("rr_lon")
        if not in_bbox(lat, lon, bbox):  # круг шире bbox — отрезаем лишнее
            continue
        alt = a.get("alt_baro")
        on_ground = alt == "ground"
        if isinstance(alt, (int, float)):
            alt_m = round(alt * FT)
        elif isinstance(a.get("alt_geom"), (int, float)):
            alt_m = round(a["alt_geom"] * FT)
        else:
            alt_m = 0 if on_ground else None
        vrate = a.get("baro_rate", a.get("geom_rate"))
        rows.append({
            "icao24": a.get("hex", "").lstrip("~"), "callsign": (a.get("flight") or "").strip(),
            "registration": a.get("r", ""), "type": a.get("t", ""),
            "lat": lat, "lon": lon, "alt_m": alt_m, "on_ground": on_ground,
            "speed_ms": round(a["gs"] * KT, 1) if a.get("gs") is not None else None,
            "track": a.get("track"),
            "vrate_ms": round(vrate * FPM, 2) if vrate is not None else None,
            "squawk": a.get("squawk", ""), "pos_age_s": a.get("seen_pos"),
            "pos_source": a.get("type", ""), "nac_p": a.get("nac_p"), "nic": a.get("nic"),
        })
    return rows


SOURCES = {"opensky": fetch_opensky, "adsblol": fetch_adsblol, "adsbfi": fetch_adsbfi}


# ---------------------------------------------------------------- сбор

def append_csv(path, fields, rows):
    new = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if new:
            w.writeheader()
        w.writerows(rows)


def poll_once(regions):
    os.makedirs(OUT_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cov_rows, ac_rows = [], []
    for region in regions:
        bbox = REGIONS[region]
        for source, fetch in SOURCES.items():
            t0 = time.monotonic()
            error = ""
            try:
                rows = fetch(bbox)
            except urllib.error.HTTPError as e:
                rows, error = [], f"HTTP {e.code}"
            except Exception as e:  # сеть, таймаут, битый JSON — фиксируем, а не падаем
                rows, error = [], f"{type(e).__name__}: {e}"[:200]
            latency = round(time.monotonic() - t0, 2)
            fresh = [r for r in rows if r["pos_age_s"] is not None and r["pos_age_s"] <= FRESH_SEC]
            cov = {
                "ts_utc": ts, "region": region, "source": source,
                # при ошибке количества пустые, чтобы не спутать сбой с пустым небом
                "n_total": "" if error else len(rows),
                "n_airborne": "" if error else sum(not r["on_ground"] for r in rows),
                "n_fresh": "" if error else len(fresh),
                "n_gps_degraded": "" if error else sum(r["nac_p"] == 0 for r in rows),
                "latency_s": latency, "error": error,
            }
            cov_rows.append(cov)
            ac_rows += [{"ts_utc": ts, "region": region, "source": source, **r} for r in rows]
            status = error or f"{len(rows):3d} бортов ({len(fresh)} свежих)"
            time.sleep(1)  # adsb.fi просит не чаще 1 запроса в секунду
            print(f"{ts}  {region:8s} {source:8s} {status}", flush=True)
    append_csv(COVERAGE_CSV, COVERAGE_FIELDS, cov_rows)
    append_csv(AIRCRAFT_CSV, AIRCRAFT_FIELDS, ac_rows)


def run_loop(interval, regions):
    print(f"Опрос каждые {interval} с, данные в {OUT_DIR}. Ctrl+C для остановки.", flush=True)
    try:
        while True:
            started = time.monotonic()
            poll_once(regions)
            time.sleep(max(0, interval - (time.monotonic() - started)))
    except KeyboardInterrupt:
        print("\nОстановлено.")


# ---------------------------------------------------------------- отчёт

def report():
    if not os.path.exists(COVERAGE_CSV):
        sys.exit(f"Нет данных: {COVERAGE_CSV}. Сначала запустите сбор.")
    with open(COVERAGE_CSV, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    keys = sorted({(r["region"], r["source"]) for r in rows})
    by_hour = defaultdict(lambda: defaultdict(list))  # (region, source) -> hour -> [n_fresh]
    stats = defaultdict(lambda: {"polls": 0, "errors": 0, "total": 0, "fresh": 0, "gps_bad": 0})
    for r in rows:
        k = (r["region"], r["source"])
        st = stats[k]
        st["polls"] += 1
        if r["error"]:
            st["errors"] += 1
            continue
        st["total"] += int(r["n_total"])
        st["fresh"] += int(r["n_fresh"])
        st["gps_bad"] += int(r["n_gps_degraded"] or 0)
        hour = datetime.strptime(r["ts_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc).astimezone(MSK).hour
        by_hour[k][hour].append(int(r["n_fresh"]))

    print(f"Период: {rows[0]['ts_utc']} … {rows[-1]['ts_utc']}, строк: {len(rows)}\n")
    print(f"{'регион/источник':20s} {'опросов':>7s} {'ошибок':>6s} {'ср.всего':>8s} "
          f"{'ср.свежих':>9s} {'GPS плохой':>10s}")
    for k in keys:
        st = stats[k]
        ok = st["polls"] - st["errors"]
        avg = lambda v: f"{v / ok:.1f}" if ok else "—"
        gps = f"{100 * st['gps_bad'] / st['total']:.0f}%" if st["total"] and k[1] != "opensky" else "—"
        print(f"{k[0] + '/' + k[1]:20s} {st['polls']:7d} {st['errors']:6d} {avg(st['total']):>8s} "
              f"{avg(st['fresh']):>9s} {gps:>10s}")

    print("\nСреднее число бортов со свежей позицией (≤60 с) по часам, МСК:")
    print(f"{'час':>4s} " + " ".join(f"{k[0] + '/' + k[1]:>16s}" for k in keys))
    for h in range(24):
        if not any(by_hour[k][h] for k in keys):
            continue
        cells = []
        for k in keys:
            v = by_hour[k][h]
            cells.append(f"{sum(v) / len(v):16.1f}" if v else f"{'—':>16s}")
        print(f"{h:4d} " + " ".join(cells))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--interval", type=int, default=600, help="интервал опроса, с (по умолчанию 600)")
    p.add_argument("--once", action="store_true", help="один опрос и выход")
    p.add_argument("--regions", default=DEFAULT_REGIONS,
                   help=f"регионы через запятую из: {', '.join(REGIONS)} (по умолчанию {DEFAULT_REGIONS})")
    p.add_argument("--report", action="store_true", help="сводка по собранным данным")
    args = p.parse_args()
    regions = [r.strip() for r in args.regions.split(",") if r.strip()]
    unknown = [r for r in regions if r not in REGIONS]
    if unknown:
        p.error(f"неизвестные регионы: {', '.join(unknown)}; доступны: {', '.join(REGIONS)}")
    if args.report:
        report()
    elif args.once:
        poll_once(regions)
    else:
        run_loop(args.interval, regions)


if __name__ == "__main__":
    main()
