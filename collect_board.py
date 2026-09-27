#!/usr/bin/env python3
"""Сбор табло аэропорта Кейптауна с сайта ACSA (airports.co.za).

Табло отдаёт расписание и фактические времена: чего нет в ADS-B. Сравнение этих
двух источников даёт задержки рейсов.

Сайт не отдаёт JSON: страница построена на ASP.NET и возвращает результаты поиска
в HTML после отправки формы. Скрипт повторяет эту форму (расширенный поиск за сутки)
и разбирает карточки рейсов.

За один прогон делается два запроса: вылеты из Кейптауна и прилёты в Кейптаун.
Результат ложится в data/raw/board/ГГГГММДДTЧЧММССZ_cpt.json.

Запуск:
    python3 collect_board.py               рейсы за сегодня
    python3 collect_board.py --date 2026-09-28
    python3 collect_board.py --dry-run     показать запрос, ничего не сохраняя

Табло меняется медленно, поэтому запускать его чаще одного-двух раз в день смысла нет.
"""

import argparse
import html
import http.cookiejar
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

SAST = timezone(timedelta(hours=2))
BOARD_URL = ("https://www.airports.co.za/airports/cape-town-international-airport"
             "/the-airport/flight-information")
AIRPORT = "Cape Town"
USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/141.0 Safari/537.36")
TIMEOUT = 90

# префикс полей веб-части табло, меняется при переверстке страницы
FIELD_PREFIX = "ctl00$ctl49$g_3d254c0d_a345_4dc8_b11b_9b55f87a5892$ctl00$"

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(ROOT, "data", "raw", "board")


def ssl_context():
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        pass
    if os.path.exists("/etc/ssl/cert.pem"):
        return ssl.create_default_context(cafile="/etc/ssl/cert.pem")
    return ssl.create_default_context()


def make_opener():
    """Сайт на SharePoint, ему нужны cookies сессии и токены из формы."""
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cj),
        urllib.request.HTTPSHandler(context=ssl_context()))
    opener.addheaders = [("User-Agent", USER_AGENT)]
    return opener


def hidden_value(page, name):
    m = re.search(rf'name="{re.escape(name)}"[^>]*value="([^"]*)"', page)
    return m.group(1) if m else ""


def build_form(page, date, direction):
    """direction: departures (из Кейптауна) или arrivals (в Кейптаун)."""
    p = FIELD_PREFIX
    form = {
        "__EVENTTARGET": "", "__EVENTARGUMENT": "", "__REQUESTDIGEST": "noDigest",
        "__VIEWSTATE": hidden_value(page, "__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": hidden_value(page, "__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION": hidden_value(page, "__EVENTVALIDATION"),
        "MSOWebPartPage_PostbackSource": "", "MSOTlPn_SelectedWpId": "", "MSOTlPn_View": "0",
        "MSOTlPn_ShowSettings": "False", "MSOGallery_SelectedLibrary": "",
        "MSOGallery_FilterString": "", "MSOTlPn_Button": "none",
        "MSOSPWebPartManager_DisplayModeName": "Browse",
        "MSOSPWebPartManager_ExitingDesignMode": "false", "MSOWebPartPage_Shared": "",
        "MSOLayout_LayoutChanges": "", "MSOLayout_InDesignMode": "",
        "_wpSelected": "", "_wzSelected": "",
        "MSOSPWebPartManager_OldDisplayModeName": "Browse",
        "MSOSPWebPartManager_StartWebPartEditingName": "false",
        "MSOSPWebPartManager_EndWebPartEditing": "false",
        p + "hdnSearchMode": "advanced",
        p + "hdnFlightNumber": "",
        p + "hdnArrivalDeparture": "both",
        p + "hdnFromAirport": AIRPORT if direction == "departures" else "",
        p + "hdnToAirport": AIRPORT if direction == "arrivals" else "",
        p + "hdnFlightType": "",
        p + "hdnAirline": "",
        p + "hdnDateFrom": date, p + "hdnDateTo": date,
        p + "hdnTimeFrom": "00:00", p + "hdnTimeTo": "23:59",
        p + "btnSearchHidden": "",
    }
    return urllib.parse.urlencode(form).encode()


def clean(text):
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", text))).strip()


def parse_flights(page, direction):
    """Разбирает карточки рейсов. Подписи полей берём как есть, чтобы ничего не потерять."""
    flights = []
    cards = page.split('<div class="flight-card">')[1:]
    for card in cards:
        card = card.split('class="results-')[0]  # хвост после последней карточки
        flight = {"direction": direction}
        for key, pattern in (("flight_number", r'class="flight-number">(.*?)</div>'),
                             ("airline", r'class="flight-airline">(.*?)</div>'),
                             ("status", r'class="status-badge[^"]*">(.*?)</div>')):
            m = re.search(pattern, card, re.S)
            if m:
                flight[key] = clean(m.group(1))
        for label, value in re.findall(r'class="detail-label">(.*?)</div>\s*'
                                       r'<div class="detail-value">(.*?)</div>', card, re.S):
            key = clean(label).lower().replace(" ", "_").replace("-", "_")
            flight[key] = clean(value)
        if flight.get("flight_number"):
            flights.append(flight)
    return flights


def results_count(page):
    m = re.search(r'class="results-count">\s*([\d\s]+)\s*flights found', page)
    return int(m.group(1).replace(" ", "")) if m else None


def collect(date, dry_run=False):
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"Табло CPT за {date}, снимок {run_id}")
    if dry_run:
        print(f"  POST {BOARD_URL}")
        print(f"  вылеты: hdnFromAirport={AIRPORT}, прилёты: hdnToAirport={AIRPORT}, "
              f"даты {date}, время 00:00-23:59")
        return 0

    opener = make_opener()
    entries, flights, failed = [], [], 0
    for direction in ("departures", "arrivals"):
        started = time.monotonic()
        entry = {"direction": direction, "date": date, "attempts": 0}
        for attempt in range(2):  # сайт изредка отдаёт пустой результат, пробуем ещё раз
            entry["attempts"] = attempt + 1
            entry.pop("error", None)
            try:
                page = opener.open(BOARD_URL, timeout=TIMEOUT).read().decode("utf-8", "replace")
                req = urllib.request.Request(
                    BOARD_URL, data=build_form(page, date, direction),
                    headers={"Content-Type": "application/x-www-form-urlencoded",
                             "Referer": BOARD_URL})
                result = opener.open(req, timeout=TIMEOUT).read().decode("utf-8", "replace")
            except urllib.error.HTTPError as e:
                entry["error"] = f"HTTP {e.code}"
            except Exception as e:
                entry["error"] = f"{type(e).__name__}: {e}"[:200]
            else:
                found = parse_flights(result, direction)
                entry.update(reported=results_count(result), parsed=len(found),
                             bytes=len(result.encode("utf-8")))
                if found:
                    flights += found
                    break
            time.sleep(5)
        entry["duration_s"] = round(time.monotonic() - started, 2)
        entries.append(entry)

        if "error" in entry:
            failed += 1
            print(f"  {direction:11s} ошибка: {entry['error']}")
        else:
            retry = " со второй попытки" if entry["attempts"] > 1 else ""
            print(f"  {direction:11s} {entry['parsed']} рейсов "
                  f"(сайт сообщает {entry['reported']}){retry}")
            if not entry["parsed"]:
                failed += 1
        time.sleep(2)

    if not flights and failed:
        print("Ничего не собрано.")
        return 1

    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f"{run_id}_cpt.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"run_id": run_id, "airport": AIRPORT, "date": date,
                   "source": "acsa", "url": BOARD_URL,
                   "requests": entries, "flights": flights}, f, ensure_ascii=False, indent=2)
    print(f"Сохранено в data/raw/board/{os.path.basename(path)}, рейсов: {len(flights)}")
    return 1 if failed else 0


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", default=datetime.now(SAST).strftime("%Y-%m-%d"),
                   help="дата в формате ГГГГ-ММ-ДД, по умолчанию сегодня по времени Кейптауна")
    p.add_argument("--dry-run", action="store_true", help="показать запрос и выйти")
    args = p.parse_args()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", args.date):
        p.error("дата должна быть в формате ГГГГ-ММ-ДД")
    sys.exit(collect(args.date, args.dry_run))


if __name__ == "__main__":
    main()
