#!/usr/bin/env python3
"""Наполнение каталога OpenMetadata: технические, операционные и бизнес-метаданные.

Каталог собирается из трёх источников, ничего не выдумывая:

  технические    состав столбцов, ключи сортировки, объёмы и профили читаются
                 из системных таблиц ClickHouse, образцы строк — из самих таблиц;
  операционные   состав задач берётся из описаний DAGов, статусы последних запусков —
                 из API Airflow, результаты проверок — из таблицы dds_dq_results;
  бизнесовые     домен, глоссарий, классификация слоёв, владелец и уровень критичности
                 задаются здесь: это знание о данных, которого в самих данных нет.

Сбор агентом OpenMetadata не используется: его контейнер — это второй Airflow, а линейку
по скриптам он всё равно не построил бы. Всё делается через REST API.

Запуск на хосте, когда подняты ClickHouse, Airflow и OpenMetadata:

    docker compose -f docker-compose.openmetadata.yml up -d
    python3 tools/openmetadata_catalog.py

Повторный запуск перезаписывает сущности, дублей не возникает.
"""

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

OM = "http://localhost:8585/api/v1"
OM_USER, OM_PASSWORD = "admin@open-metadata.org", "admin"

CLICKHOUSE = "http://localhost:8123"
CH_USER, CH_PASSWORD, CH_DATABASE = "ods", "ods", "ods"

AIRFLOW = "http://localhost:8080"
AIRFLOW_USER, AIRFLOW_PASSWORD = "airflow", "airflow"

FILES_SERVICE = "adsb-raw-files"
DB_SERVICE = "adsb-clickhouse"
PIPELINE_SERVICE = "adsb-airflow"
SCHEMA_FQN = f"{DB_SERVICE}.{CH_DATABASE}.{CH_DATABASE}"

TEAM = "adsb-data-team"
DOMAIN = "air-traffic"
GLOSSARY = "air-traffic-glossary"
LAYERS = "Слой"

# ---------------------------------------------------------------- бизнес-знание

GLOSSARY_TERMS = {
    "ADS-B": ("Стандарт, по которому самолёт примерно дважды в секунду передаёт в эфир "
              "координаты, высоту, скорость и позывной. Приём ведут волонтёрские сети, "
              "поэтому покрытие зависит от расположения приёмников.", ["ADSB"]),
    "METAR": ("Стандартная сводка погоды аэропорта, выходит раз в час. Содержит ветер, "
              "видимость, облачность, температуру и давление.", ["сводка погоды"]),
    "Борт": ("Конкретное воздушное судно. Опознаётся по адресу передатчика icao24, "
             "который не меняется за всю жизнь судна, в отличие от позывного и регистрации.",
             ["воздушное судно", "aircraft"]),
    "Позывной": ("Опознавательный индекс рейса в эфире. Меняется от рейса к рейсу, "
                 "поэтому не годится как идентификатор борта.", ["callsign"]),
    "Снимок": ("Один прогон сборщика: опрос источников в конкретный момент времени. "
               "Идентифицируется меткой run_id и служит единицей загрузки и дедупликации.",
               ["run", "прогон сбора"]),
    "Задержка рейса": ("Разница между фактическим и плановым временем в минутах. Считается "
                       "только для вылетевших и севших рейсов; отрицательное значение "
                       "означает опережение расписания. Опоздавшим считается рейс "
                       "с задержкой больше 15 минут.", ["delay", "опоздание"]),
    "Эксклюзивное наблюдение": ("Наблюдение, в котором борт в этом снимке и этой зоне видела "
                                "ровно одна сеть приёмников. Доля таких наблюдений показывает, "
                                "что теряется при отказе от источника.", ["exclusive"]),
    "Рабочая полоса": ("Полоса, которой пользуются при текущем ветре. Взлёт и посадка идут "
                       "против ветра, поэтому рабочей считается полоса, курс которой ближе "
                       "всего к направлению ветра.", ["runway in use"]),
    "Боковая составляющая ветра": ("Часть скорости ветра, направленная поперёк полосы. "
                                   "Именно она ограничивает посадку.", ["crosswind"]),
    "Зерно факта": ("Что именно считается одной строкой таблицы фактов. У позиций это снимок, "
                    "источник, зона и борт; у рейса — рейс за сутки.", ["grain"]),
    "Медленно меняющееся измерение": ("Способ хранить историю атрибутов. Тип 2 заводит новую "
                                      "версию строки при каждом изменении и закрывает "
                                      "предыдущую интервалом действия.", ["SCD", "SCD2"]),
    "Вырожденное измерение": ("Поле, которое ведёт себя как ключ измерения, но собственных "
                              "атрибутов не имеет, поэтому хранится прямо в факте.",
                              ["degenerate dimension"]),
    "Сырой слой": ("Данные в том виде, в каком пришли от источника: разобраны только поля "
                   "для запросов, ответ целиком сохранён рядом.", ["ODS", "staging"]),
    "Детальный слой": ("Очищенные и связанные данные: факты, измерения, история версий. "
                       "Приведён к третьей нормальной форме.", ["DDS", "core"]),
    "Витрина": ("Готовый ответ на один аналитический вопрос. Рассчитана на прямое чтение "
                "без соединений.", ["data mart", "DM"]),
}

# термин глоссария вешается на столбцы, которые его выражают
TERM_COLUMNS = {
    "Задержка рейса": ["delay_min", "avg_delay_min", "median_delay_min", "p90_delay_min",
                       "worst_delay_min", "late_share"],
    "Позывной": ["callsign"],
    "Борт": ["icao24", "aircraft_sk", "registration"],
    "Снимок": ["run_id", "snapshot_ts", "last_run_id", "snapshots_seen"],
    "Эксклюзивное наблюдение": ["exclusive_observations", "exclusive_share"],
    "Рабочая полоса": ["preferred_runway"],
    "Боковая составляющая ветра": ["avg_crosswind_kt"],
    "Медленно меняющееся измерение": ["valid_from", "valid_to", "is_current"],
}

LAYER_TAGS = {
    "Сырой": "Слой ODS: данные как пришли от источника",
    "Детальный": "Слой DDS: очищенные факты и измерения",
    "Витрина": "Слой DM: готовые ответы на аналитические вопросы",
    "Файлы": "Сырые снимки на диске до загрузки в хранилище",
}

# ---------------------------------------------------------------- состав каталога

CONTAINERS = {
    "positions": "Снимки позиций бортов из OpenSky, adsb.lol и adsb.fi, data/raw/positions",
    "board": "Снимки табло аэропорта Кейптауна, data/raw/board",
    "weather": "Сводки METAR по аэропортам ЮАР, data/raw/weather",
}

# таблица: описание, слой, уровень критичности
TABLES = {
    "ods_positions": ("Сырые позиции бортов из трёх сетей приёмников. Разобраны только поля, "
                      "по которым идут запросы; ответ источника целиком лежит в колонке raw, "
                      "и из неё же слой DDS достаёт регистрацию и тип борта.",
                      "Сырой", "Tier3"),
    "ods_flights": ("Сырые рейсы с табло аэропорта: расписание, фактическое время и статус. "
                    "Единственный источник плановых времён — в ADS-B расписания нет.",
                    "Сырой", "Tier3"),
    "ods_metar": ("Сырые сводки погоды по четырём аэропортам ЮАР. Запрос берёт 12 часов "
                  "истории, поэтому соседние снимки перекрываются.", "Сырой", "Tier3"),
    "dds_dim_date": ("Календарь на 1500 дней: день недели, квартал, признак выходного. "
                     "Не зависит от данных, заполняется один раз.", "Детальный", "Tier3"),
    "dds_dim_source": ("Три сети приёмников. Номер ключа задаёт приоритет при слиянии: "
                       "1 — adsb.lol, 2 — adsb.fi, 3 — OpenSky.", "Детальный", "Tier3"),
    "dds_dim_region": ("Две зоны опроса: Кейптаун с аэропортом и вся ЮАР.",
                       "Детальный", "Tier3"),
    "dds_dim_airline": ("Авиакомпании, встреченные на табло. Тип 1: новое название "
                        "перезаписывает старое, история не ведётся.", "Детальный", "Tier2"),
    "dds_dim_airport": ("Аэропорты с табло и коды ICAO станций погоды. Мост между двумя "
                        "источниками: табло знает названия, METAR — только коды.",
                        "Детальный", "Tier2"),
    "dds_dim_aircraft_type": ("Типы самолётов. Вынесены из измерения бортов отдельной "
                              "таблицей ради третьей нормальной формы: описание типа зависит "
                              "от кода типа, а не от конкретного борта.", "Детальный", "Tier3"),
    "dds_dim_aircraft": ("Борта с историей версий по типу 2. Одна строка — одна версия "
                         "атрибутов, а не борт: при смене регистрации, типа или эксплуатанта "
                         "прошлая версия закрывается и открывается новая.",
                         "Детальный", "Tier2"),
    "dds_fact_position": ("Очищенная позиция борта так, как её увидела одна сеть. Зерно: "
                          "снимок, источник, зона, борт. Три сети, увидевшие один борт, "
                          "дают три строки.", "Детальный", "Tier2"),
    "dds_fact_aircraft_snapshot": ("Борт в снимке после слияния трёх сетей: одна строка "
                                   "на борт. Координаты берутся у источника с высшим "
                                   "приоритетом, число увидевших сетей сохраняется мерой.",
                                   "Детальный", "Tier2"),
    "dds_fact_flight": ("Рейс за сутки с рассчитанной задержкой. Все снимки табло схлопнуты "
                        "в одну строку: берётся состояние из самого позднего снимка.",
                        "Детальный", "Tier1"),
    "dds_fact_weather": ("Наблюдение METAR без перекрытий: дубли сняты по станции и времени "
                         "наблюдения.", "Детальный", "Tier2"),
    "dds_dq_results": ("Журнал проверок качества детального слоя: по строке на проверку "
                       "и прогон. Движок без схлопывания — история должна накапливаться.",
                       "Детальный", "Tier3"),
    "dm_airport_load_hourly": ("Витрина: загрузка аэропорта по часам суток и задержки в эти "
                               "часы. Время переведено в местное — профиль суток имеет смысл "
                               "только в нём.", "Витрина", "Tier1"),
    "dm_airline_punctuality": ("Витрина: пунктуальность авиакомпаний. Среднее, медиана "
                               "и 90-й перцентиль рядом, потому что редкие сильные опоздания "
                               "сдвигают среднее.", "Витрина", "Tier1"),
    "dm_source_coverage": ("Витрина: что даёт каждая сеть приёмников и оправдан ли сбор "
                           "из трёх источников.", "Витрина", "Tier1"),
    "dm_weather_runway": ("Витрина: роза ветров и рабочая полоса. Для Кейптауна курсы полос "
                          "известны, для остальных аэропортов поле полосы пустое.",
                          "Витрина", "Tier1"),
    "dm_fleet_mix": ("Витрина: состав наблюдаемого парка по типам и доля малой авиации. "
                     "Категория определяется по средней высоте наблюдений.",
                     "Витрина", "Tier1"),
    "dm_traffic_daily": ("Витрина: трафик по дням. Сводит три факта, поэтому дни, когда "
                         "работали не все сборщики, видны пропусками.", "Витрина", "Tier1"),
}

# описания столбцов по имени: имя повторяется в нескольких таблицах и значит одно и то же
COLUMN_NOTES = {
    "run_id": "Метка снимка сбора в UTC; по ней отсекается повторная загрузка",
    "loaded_at": "Когда строка попала в базу; отличается от времени сбора",
    "raw": "Ответ источника целиком — страховка на случай, если понадобится неразобранное поле",
    "source": "Сеть приёмников: opensky, adsblol или adsbfi",
    "region": "Зона опроса: cpt — Кейптаун, za — вся ЮАР",
    "region_code": "Зона опроса: cpt — Кейптаун, za — вся ЮАР",
    "fetched_at": "Время запроса в UTC: ставит сборщик, потому что источники отдают время по-разному",
    "icao24": "24-битный адрес передатчика — неизменный идентификатор борта",
    "callsign": "Позывной рейса; меняется от рейса к рейсу и бывает пустым",
    "lat": "Широта в градусах",
    "lon": "Долгота в градусах",
    "alt_m": "Высота в метрах; у adsb-источников приведена из футов",
    "on_ground": "Признак, что борт на земле: отделяет руление от полёта",
    "speed_ms": "Путевая скорость в метрах в секунду",
    "track": "Курс в градусах; нужен для анализа заходов на посадку",
    "flight_date": "Дата рейса",
    "direction": "Направление: departures — вылет, arrivals — прилёт",
    "flight_number": "Номер рейса; вырожденное измерение, собственных атрибутов не имеет",
    "airline": "Авиакомпания строкой, как написано на табло",
    "schedule_time": "Время по расписанию, местное, в формате ЧЧ:ММ",
    "updated_time": "Фактическое время, местное; достоверно только для вылетевших и севших",
    "status": "Состояние рейса: объясняет, почему задержка бывает неизвестна",
    "origin": "Пункт отправления",
    "destination": "Пункт назначения",
    "icao": "Код станции METAR",
    "icao_code": "Код аэропорта по ICAO",
    "report_time": "Время наблюдения",
    "wind_dir": "Направление ветра в градусах — откуда дует",
    "wind_speed": "Скорость ветра в узлах",
    "visibility": "Видимость строкой: METAR пишет 6+ в смысле «шесть и больше»",
    "temp_c": "Температура в градусах Цельсия",
    "altimeter": "Давление в гектопаскалях",
    "raw_ob": "Исходная строка METAR: в ней облачность и явления погоды, не вынесенные в столбцы",
    "date_key": "Ссылка на календарь: дата числом вида 20261002",
    "source_key": "Ссылка на источник; номер задаёт приоритет при слиянии",
    "aircraft_sk": "Ссылка на версию борта, действовавшую на момент снимка",
    "airline_key": "Ссылка на авиакомпанию; 0 означает «неизвестна»",
    "airport_key": "Ссылка на аэропорт; 0 означает «неизвестен»",
    "type_code": "Код типа самолёта",
    "type_desc": "Расшифровка типа: хранится один раз на весь проект",
    "snapshot_ts": "Время снимка в UTC: по нему ищется версия борта и режутся партиции",
    "observed_ts": "Время наблюдения в UTC: по нему снимаются перекрытия запросов",
    "row_version": "Версия строки в миллисекундах: движок оставляет строку с большей",
    "registration": "Бортовой номер вида ZS-ABC; меняется при перерегистрации",
    "operator": "Эксплуатант; меняется при передаче борта в лизинг",
    "valid_from": "С какого момента версия действует; у первой — 1970 год",
    "valid_to": "По какой момент действует; у текущей — 2100 год",
    "is_current": "Признак текущей версии: выводится из valid_to, но ускоряет запросы",
    "scheduled_ts": "Время по расписанию в UTC",
    "actual_ts": "Фактическое время в UTC; есть только у вылетевших и севших",
    "delay_min": "Задержка в минутах; переход через полночь исправлен, выбросы отброшены",
    "snapshots_seen": "В скольких снимках табло встретился рейс — мера доверия к состоянию",
    "last_run_id": "Из какого снимка взято состояние: путь назад к исходному файлу",
    "flight_key": "Ключ рейса: хеш от даты, направления, номера и времени по расписанию",
    "best_source_key": "Чьи значения победили при слиянии",
    "sources_count": "Сколькими сетями виден борт — информация, теряющаяся при слиянии",
    "in_cpt": "Попадал ли борт в зону Кейптауна: после слияния зона как измерение исчезает",
    "wind_dir_deg": "Направление ветра в градусах — откуда дует",
    "wind_speed_kt": "Скорость ветра в узлах",
    "visibility_sm": "Видимость в милях; строка 6+ приведена к числу",
    "altimeter_hpa": "Давление в гектопаскалях: нужно для перевода высоты в истинную",
    "year": "Год", "quarter": "Квартал", "month": "Месяц", "day": "День месяца",
    "day_of_week": "День недели числом, 1 — понедельник",
    "day_name": "Название дня недели по-русски",
    "is_weekend": "Выходной день: самый частый разрез вынесен в готовый признак",
    "date": "Дата",
    "source_name": "Имя источника, как в слое ODS",
    "airline_name": "Название авиакомпании",
    "airport_name": "Название аэропорта, как на табло",
    "region_name": "Описание зоны опроса",
    "description": "Чем полезен источник",
}

# столбцы, встречающиеся в одной таблице и требующие своего пояснения
TABLE_COLUMN_NOTES = {
    "dds_dq_results": {
        "run_ts": "Когда прогонялись проверки",
        "area": "Область проверок: positions, flights, weather или integrity",
        "check_name": "Имя проверки",
        "severity": "error валит DAG, warn только отмечается",
        "status": "Итог: ok, fail или warn",
        "value": "Что вернул SQL проверки: каждая обязана вернуть одно число",
        "rule": "Правило в читаемом виде, например «<= 0»",
    },
    "dm_airport_load_hourly": {
        "hour_local": "Час суток по времени Кейптауна",
        "flights": "Всего рейсов за период наблюдения",
        "days": "Дней наблюдения — знаменатель для среднего",
        "flights_per_day": "Рейсов в день: делится на все дни, а не на дни с рейсом в этот час",
        "avg_delay_min": "Средняя задержка рейсов этого часа",
        "late_share": "Доля рейсов с задержкой больше 15 минут",
    },
    "dm_airline_punctuality": {
        "flights": "Всего рейсов авиакомпании в этом направлении",
        "delays_known": "По скольким рейсам известно фактическое время",
        "avg_delay_min": "Средняя задержка в минутах",
        "median_delay_min": "Медианная задержка: показывает типичный рейс",
        "p90_delay_min": "90-й перцентиль: показывает тяжёлый хвост",
        "late_share": "Доля опоздавших больше чем на 15 минут",
        "early_share": "Доля ушедших раньше расписания",
        "worst_delay_min": "Худшая задержка за период",
    },
    "dm_source_coverage": {
        "snapshots": "В скольких снимках участвовал источник",
        "observations": "Сколько строк наблюдений дал",
        "aircraft_seen": "Сколько разных бортов видел",
        "avg_per_snapshot": "Сколько бортов в среднем отдаёт за запрос",
        "avg_alt_m": "Средняя высота наблюдений: показывает характер покрытия",
        "exclusive_observations": "Наблюдений, где борт видел только этот источник",
        "exclusive_share": "Доля таких наблюдений — цена отказа от источника",
    },
    "dm_weather_runway": {
        "sector_deg": "Сектор розы ветров в градусах, шаг 45",
        "wind_sector": "Сектор словами: С, СВ, В, ЮВ, Ю, ЮЗ, З, СЗ",
        "observations": "Сколько наблюдений попало в сектор",
        "share": "Доля сектора среди всех наблюдений аэропорта",
        "avg_wind_kt": "Средняя скорость ветра в секторе, узлы",
        "max_wind_kt": "Максимальная скорость ветра в секторе",
        "preferred_runway": "Полоса, курс которой ближе всего к направлению ветра",
        "avg_crosswind_kt": "Средняя боковая составляющая: чем меньше, тем удачнее полоса",
    },
    "dm_fleet_mix": {
        "aircraft": "Сколько разных бортов этого типа наблюдалось",
        "observations": "Сколько наблюдений дали эти борта",
        "avg_alt_m": "Средняя высота наблюдений типа",
        "max_alt_m": "Максимальная высота наблюдений типа",
        "avg_speed_ms": "Средняя путевая скорость",
        "aircraft_share": "Доля типа среди всех наблюдавшихся бортов",
        "category": "Магистральные или малая авиация — эвристика по средней высоте",
    },
    "dm_traffic_daily": {
        "departures": "Вылетов за день",
        "arrivals": "Прилётов за день",
        "avg_delay_min": "Средняя задержка за день",
        "late_share": "Доля опоздавших за день",
        "aircraft_seen": "Сколько разных бортов наблюдалось",
        "observations": "Сколько записей наблюдений собрано",
        "snapshots": "Сколько снимков сделано за день",
        "avg_wind_kt": "Средний ветер в Кейптауне за день",
        "max_wind_kt": "Максимальный ветер за день",
    },
}

# ---------------------------------------------------------------- процессы Airflow

PIPELINES = {
    "ods_positions": ("Опрос трёх API позиций и загрузка снимка в сырой слой",
                      ["extract", "load"]),
    "ods_board": ("Сбор табло аэропорта за сутки и загрузка рейсов в сырой слой",
                  ["extract", "load"]),
    "ods_weather": ("Сбор сводок METAR и загрузка в сырой слой", ["extract", "load"]),
    "ods_archive": ("Разовая загрузка всего, что собрано сборщиками вручную",
                    ["positions", "flights", "metar"]),
    "dds_positions": ("Детальный слой: версии бортов, очищенные позиции, слияние сетей",
                      ["ods_ready", "schema", "dim_aircraft", "fact_position",
                       "fact_aircraft_snapshot", "publish"]),
    "dds_flights": ("Детальный слой: справочники табло и факт рейса с задержкой",
                    ["ods_ready", "schema", "dim_airline_airport", "fact_flight", "publish"]),
    "dds_weather": ("Детальный слой: наблюдения METAR без перекрытий",
                    ["ods_ready", "schema", "fact_weather", "publish"]),
    "dds_quality": ("Проверка качества детального слоя: 34 проверки в четырёх областях",
                    ["check", "verdict"]),
    "dm_marts": ("Пересборка шести витрин из детального слоя",
                 ["dds_ready", "schema", "airport_load", "airline_punctuality",
                  "source_coverage", "weather_runway", "fleet_mix", "traffic_daily", "verify"]),
}

# ---------------------------------------------------------------- линейка

LINEAGE = [
    ("positions", "ods_positions", "Разбор JSON в колонки", "ods_positions"),
    ("board", "ods_flights", "Разбор карточек рейсов", "ods_board"),
    ("weather", "ods_metar", "Разбор сводок METAR", "ods_weather"),

    ("ods_positions", "dds_dim_aircraft", "sql/dds/20_dim_aircraft.sql: версии по SCD2",
     "dds_positions"),
    ("ods_positions", "dds_dim_aircraft_type", "sql/dds/20_dim_aircraft.sql: справочник типов",
     "dds_positions"),
    ("ods_positions", "dds_fact_position", "sql/dds/30_fact_position.sql: очистка и обогащение",
     "dds_positions"),
    ("dds_dim_aircraft", "dds_fact_position", "sql/dds/30_fact_position.sql: версия на момент снимка",
     "dds_positions"),
    ("dds_dim_source", "dds_fact_position", "sql/dds/30_fact_position.sql: ключ источника",
     "dds_positions"),
    ("dds_dim_date", "dds_fact_position", "sql/dds/30_fact_position.sql: ключ даты",
     "dds_positions"),
    ("dds_fact_position", "dds_fact_aircraft_snapshot",
     "sql/dds/31_fact_aircraft_snapshot.sql: слияние трёх сетей", "dds_positions"),

    ("ods_flights", "dds_dim_airline", "sql/dds/40_dim_airline_airport.sql: справочник, тип 1",
     "dds_flights"),
    ("ods_flights", "dds_dim_airport", "sql/dds/40_dim_airline_airport.sql: справочник, тип 1",
     "dds_flights"),
    ("ods_flights", "dds_fact_flight", "sql/dds/50_fact_flight.sql: последнее состояние и задержка",
     "dds_flights"),
    ("dds_dim_airline", "dds_fact_flight", "sql/dds/50_fact_flight.sql: ключ авиакомпании",
     "dds_flights"),
    ("dds_dim_airport", "dds_fact_flight", "sql/dds/50_fact_flight.sql: ключ аэропорта",
     "dds_flights"),
    ("dds_dim_date", "dds_fact_flight", "sql/dds/50_fact_flight.sql: ключ даты", "dds_flights"),

    ("ods_metar", "dds_fact_weather", "sql/dds/60_fact_weather.sql: снятие перекрытий",
     "dds_weather"),
    ("dds_dim_airport", "dds_fact_weather", "sql/dds/60_fact_weather.sql: ключ аэропорта",
     "dds_weather"),

    ("dds_fact_position", "dds_dq_results", "etl/dq.py: область positions, 9 проверок",
     "dds_quality"),
    ("dds_fact_flight", "dds_dq_results", "etl/dq.py: область flights, 7 проверок",
     "dds_quality"),
    ("dds_fact_weather", "dds_dq_results", "etl/dq.py: область weather, 6 проверок",
     "dds_quality"),

    ("dds_fact_flight", "dm_airport_load_hourly", "sql/dm/10_airport_load.sql", "dm_marts"),
    ("dds_fact_flight", "dm_airline_punctuality", "sql/dm/20_airline_punctuality.sql", "dm_marts"),
    ("dds_dim_airline", "dm_airline_punctuality", "sql/dm/20_airline_punctuality.sql", "dm_marts"),
    ("dds_fact_position", "dm_source_coverage", "sql/dm/30_source_coverage.sql", "dm_marts"),
    ("dds_dim_source", "dm_source_coverage", "sql/dm/30_source_coverage.sql", "dm_marts"),
    ("dds_fact_weather", "dm_weather_runway", "sql/dm/40_weather_runway.sql", "dm_marts"),
    ("dds_fact_aircraft_snapshot", "dm_fleet_mix", "sql/dm/50_fleet_mix.sql", "dm_marts"),
    ("dds_dim_aircraft", "dm_fleet_mix", "sql/dm/50_fleet_mix.sql", "dm_marts"),
    ("dds_dim_aircraft_type", "dm_fleet_mix", "sql/dm/50_fleet_mix.sql", "dm_marts"),
    ("dds_fact_flight", "dm_traffic_daily", "sql/dm/60_traffic_daily.sql", "dm_marts"),
    ("dds_fact_aircraft_snapshot", "dm_traffic_daily", "sql/dm/60_traffic_daily.sql", "dm_marts"),
    ("dds_fact_weather", "dm_traffic_daily", "sql/dm/60_traffic_daily.sql", "dm_marts"),
    ("dds_dim_date", "dm_traffic_daily", "sql/dm/60_traffic_daily.sql", "dm_marts"),
]

# область проверки качества относится к своей таблице
DQ_TABLES = {"positions": "dds_fact_position", "flights": "dds_fact_flight",
             "weather": "dds_fact_weather"}
DQ_PREFIX = [("position_", "dds_fact_position"), ("flight_", "dds_fact_flight"),
             ("weather_", "dds_fact_weather"), ("aircraft_type_", "dds_dim_aircraft"),
             ("scd2_", "dds_dim_aircraft")]

TYPES = [("String", "STRING"), ("UInt64", "BIGINT"), ("Int64", "BIGINT"),
         ("UInt32", "INT"), ("Int32", "INT"), ("UInt16", "INT"), ("Int16", "INT"),
         ("UInt8", "INT"), ("Int8", "INT"), ("Float64", "DOUBLE"), ("Float32", "FLOAT"),
         ("DateTime", "DATETIME"), ("Date", "DATE")]
NUMERIC = ("Int", "Float")


# ---------------------------------------------------------------- доступ к сервисам

def request(url, token=None, method="GET", payload=None, headers=None):
    data = json.dumps(payload).encode() if payload is not None else None
    head = {"Content-Type": "application/json", **(headers or {})}
    if token:
        head["Authorization"] = f"Bearer {token}"
    try:
        with urllib.request.urlopen(
                urllib.request.Request(url, data=data, method=method, headers=head),
                timeout=90) as response:
            body = response.read().decode()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {url} -> HTTP {e.code}: {e.read().decode()[:300]}") from None


def om(method, path, token, payload=None):
    return request(OM + path, token, method, payload)


def om_login():
    password = base64.b64encode(OM_PASSWORD.encode()).decode()
    return request(f"{OM}/users/login", method="POST",
                   payload={"email": OM_USER, "password": password})["accessToken"]


def airflow_token():
    try:
        return request(f"{AIRFLOW}/auth/token", method="POST",
                       payload={"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD})\
            .get("access_token")
    except Exception as e:
        print(f"  Airflow недоступен ({type(e).__name__}), статусы запусков пропускаем")
        return None


def clickhouse(query, fmt="TabSeparated"):
    url = f"{CLICKHOUSE}/?{urllib.parse.urlencode({'database': CH_DATABASE, 'query': query + ' format ' + fmt})}"
    body = request(url, headers={"X-ClickHouse-User": CH_USER, "X-ClickHouse-Key": CH_PASSWORD})
    return body


def ch_rows(query):
    url = f"{CLICKHOUSE}/?{urllib.parse.urlencode({'database': CH_DATABASE, 'query': query})}"
    req = urllib.request.Request(url, headers={"X-ClickHouse-User": CH_USER,
                                               "X-ClickHouse-Key": CH_PASSWORD})
    with urllib.request.urlopen(req, timeout=90) as response:
        text = response.read().decode().strip()
    return [line.split("\t") for line in text.split("\n") if line]


def ch_json(query):
    url = f"{CLICKHOUSE}/?{urllib.parse.urlencode({'database': CH_DATABASE, 'query': query + ' format JSONCompact'})}"
    req = urllib.request.Request(url, headers={"X-ClickHouse-User": CH_USER,
                                               "X-ClickHouse-Key": CH_PASSWORD})
    with urllib.request.urlopen(req, timeout=90) as response:
        return json.loads(response.read().decode())


def now_ms():
    return int(time.time() * 1000)


def om_type(clickhouse_type):
    for prefix, mapped in TYPES:
        if prefix in clickhouse_type:
            return mapped
    return "STRING"


# ---------------------------------------------------------------- бизнес-знание

def create_business(token):
    om("PUT", "/teams", token, {
        "name": TEAM, "displayName": "Команда данных ADS-B", "teamType": "Group",
        "description": "Отвечает за сбор, хранение и витрины по воздушному движению"})
    om("PUT", "/domains", token, {
        "name": DOMAIN, "displayName": "Воздушное движение", "domainType": "Aggregate",
        "description": "Данные о полётах и погоде в районе Кейптауна: от снимков "
                       "приёмников ADS-B до витрин пунктуальности и покрытия"})
    om("PUT", "/classifications", token, {
        "name": LAYERS, "description": "Слой хранилища, к которому относится таблица"})
    for name, description in LAYER_TAGS.items():
        om("PUT", "/tags", token, {"classification": LAYERS, "name": name,
                                   "description": description})
    om("PUT", "/glossaries", token, {
        "name": GLOSSARY, "displayName": "Воздушное движение",
        "description": "Термины предметной области: рейсы, борта, погода и устройство хранилища"})
    for term, (description, synonyms) in GLOSSARY_TERMS.items():
        om("PUT", "/glossaryTerms", token, {
            "glossary": GLOSSARY, "name": term, "description": description,
            "synonyms": synonyms})
    print(f"Бизнес-слой: домен, команда, {len(LAYER_TAGS)} тегов слоёв, "
          f"{len(GLOSSARY_TERMS)} терминов глоссария")
    return om("GET", f"/teams/name/{TEAM}", token)["id"]


def tag(fqn, source="Classification"):
    return {"tagFQN": fqn, "source": source, "labelType": "Manual", "state": "Confirmed"}


def column_tags(column):
    return [tag(f"{GLOSSARY}.{term}", "Glossary")
            for term, columns in TERM_COLUMNS.items() if column in columns]


# ---------------------------------------------------------------- технические метаданные

def table_structure():
    """Состав столбцов, типы и ключи сортировки — из системных таблиц ClickHouse."""
    columns = {}
    for table, name, type_ in ch_rows(
            "select table, name, type from system.columns "
            f"where database = '{CH_DATABASE}' order by table, position"):
        columns.setdefault(table, []).append((name, type_))
    keys = {t: k for t, k in ch_rows(
        f"select name, sorting_key from system.tables where database = '{CH_DATABASE}'")}
    return columns, keys


def table_volume():
    rows = ch_rows("select name, toString(total_rows), toString(total_bytes) "
                   f"from system.tables where database = '{CH_DATABASE}'")
    return {name: (int(r or 0), int(b or 0)) for name, r, b in rows}


def column_profile(table, columns):
    """Профиль столбцов: заполненность, число значений и границы — одним запросом."""
    parts, meta = [], []
    for name, type_ in columns:
        parts.append(f"uniqExact({name})")
        parts.append(f"countIf({name} is null)")
        meta.append((name, type_))
        if any(n in type_ for n in NUMERIC):
            parts.append(f"toString(min({name}))")
            parts.append(f"toString(max({name}))")
    data = ch_json(f"select count(), {', '.join(parts)} from {table}")["data"][0]
    total, index, profile = data[0], 1, []
    for name, type_ in meta:
        item = {"name": name, "timestamp": now_ms(), "valuesCount": total,
                "distinctCount": data[index], "nullCount": data[index + 1]}
        index += 2
        if any(n in type_ for n in NUMERIC):
            low, high = data[index], data[index + 1]
            index += 2
            try:
                item["min"], item["max"] = float(low), float(high)
            except (TypeError, ValueError):
                pass
        profile.append(item)
    return total, profile


def sample_rows(table):
    final = " final" if table.startswith("dds_") and table != "dds_dq_results" else ""
    answer = ch_json(f"select * from {table}{final} limit 5")
    return [c["name"] for c in answer["meta"]], answer["data"]


def create_tables(token, team_id, structure, keys, volume):
    entities = {}
    for name, (description, layer, tier) in TABLES.items():
        if name not in structure:
            raise RuntimeError(f"В ClickHouse нет таблицы {name}")
        sort_key = [c.strip() for c in keys.get(name, "").split(",") if c.strip()]
        notes = TABLE_COLUMN_NOTES.get(name, {})
        columns = [{
            "name": column,
            "dataType": om_type(type_),
            "dataTypeDisplay": type_,
            "dataLength": 1,
            "description": notes.get(column) or COLUMN_NOTES.get(column, ""),
            # ключ сортировки описан ограничением таблицы, здесь — обязательность значения
            "constraint": "NULL" if type_.startswith("Nullable(") else "NOT_NULL",
            "tags": column_tags(column),
        } for column, type_ in structure[name]]

        payload = {
            "name": name, "databaseSchema": SCHEMA_FQN, "description": description,
            "columns": columns,
            "owners": [{"id": team_id, "type": "team"}],
            "domains": [DOMAIN],
            "tags": [tag(f"{LAYERS}.{layer}"), tag(f"Tier.{tier}")],
        }
        if sort_key:
            payload["tableConstraints"] = [{"constraintType": "PRIMARY_KEY",
                                            "columns": sort_key}]
        answer = om("PUT", "/tables", token, payload)
        entities[name] = ("table", answer["id"])

        rows, profile = column_profile(name, structure[name])
        om("PUT", f"/tables/{answer['id']}/tableProfile", token, {
            "tableProfile": {"timestamp": now_ms(), "rowCount": rows,
                             "columnCount": len(columns),
                             "sizeInByte": volume.get(name, (0, 0))[1]},
            "columnProfile": profile})
        sample_columns, sample_data = sample_rows(name)
        om("PUT", f"/tables/{answer['id']}/sampleData", token,
           {"columns": sample_columns, "rows": sample_data})
    print(f"Технический слой: {len(entities)} таблиц с описаниями столбцов, "
          f"ключами, профилями и образцами строк")
    return entities


def create_containers(token, team_id):
    entities = {}
    for name, description in CONTAINERS.items():
        answer = om("PUT", "/containers", token, {
            "name": name, "service": FILES_SERVICE, "description": description,
            "fileFormats": ["json"], "prefix": f"/data/raw/{name}",
            "owners": [{"id": team_id, "type": "team"}],
            "domains": [DOMAIN],
            "tags": [tag(f"{LAYERS}.Файлы")]})
        entities[name] = ("container", answer["id"])
    return entities


def create_services(token):
    om("PUT", "/services/storageServices", token, {
        "name": FILES_SERVICE, "serviceType": "CustomStorage",
        "description": "Сырые снимки, которые складывают сборщики в data/raw",
        "connection": {"config": {"type": "CustomStorage"}}})
    om("PUT", "/services/databaseServices", token, {
        "name": DB_SERVICE, "serviceType": "Clickhouse",
        "description": "ClickHouse: сырой слой, детальный слой и витрины",
        "connection": {"config": {
            "type": "Clickhouse", "scheme": "clickhouse+http",
            "username": CH_USER, "password": CH_PASSWORD,
            "hostPort": "clickhouse:8123", "databaseSchema": CH_DATABASE}}})
    om("PUT", "/databases", token, {
        "name": CH_DATABASE, "service": DB_SERVICE,
        "description": "База ods: все три слоя лежат рядом, слои различаются префиксом"})
    om("PUT", "/databaseSchemas", token, {
        "name": CH_DATABASE, "database": f"{DB_SERVICE}.{CH_DATABASE}"})
    om("PUT", "/services/pipelineServices", token, {
        "name": PIPELINE_SERVICE, "serviceType": "Airflow",
        "description": "Airflow учебного стенда: девять процессов трёх слоёв",
        "connection": {"config": {"type": "Airflow", "hostPort": AIRFLOW,
                                  "connection": {"type": "Backend"}}}})


# ---------------------------------------------------------------- операционные метаданные

def create_pipelines(token, team_id, air_token):
    pipelines = {}
    for name, (description, tasks) in PIPELINES.items():
        answer = om("PUT", "/pipelines", token, {
            "name": name, "service": PIPELINE_SERVICE, "description": description,
            "sourceUrl": f"{AIRFLOW}/dags/{name}/grid",
            "owners": [{"id": team_id, "type": "team"}],
            "domains": [DOMAIN],
            "tasks": [{"name": t, "displayName": t, "taskType": "PythonOperator",
                       "downstreamTasks": tasks[i + 1:i + 2]}
                      for i, t in enumerate(tasks)]})
        pipelines[name] = answer["id"]
        if air_token:
            push_run_status(token, name, tasks, air_token)
    print(f"Операционный слой: {len(pipelines)} процессов с задачами"
          + (" и статусами последних запусков" if air_token else ""))
    return pipelines


def push_run_status(token, dag, tasks, air_token):
    """Статус последнего запуска берётся из Airflow, а не придумывается."""
    try:
        runs = request(f"{AIRFLOW}/api/v2/dags/{dag}/dagRuns?limit=1&order_by=-start_date",
                       air_token)["dag_runs"]
    except Exception:
        return
    if not runs:
        return
    run = runs[0]
    state = {"success": "Successful", "failed": "Failed"}.get(run["state"], "Pending")
    started = run.get("start_date")
    stamp = now_ms()
    if started:
        stamp = int(time.mktime(time.strptime(started[:19], "%Y-%m-%dT%H:%M:%S")) * 1000)
    om("PUT", f"/pipelines/{PIPELINE_SERVICE}.{dag}/status", token, {
        "timestamp": stamp, "executionStatus": state,
        "taskStatus": [{"name": t, "executionStatus": state} for t in tasks]})


def dq_table(area, check):
    for prefix, table in DQ_PREFIX:
        if check.startswith(prefix):
            return table
    return DQ_TABLES.get(area, "dds_dq_results")


def create_quality(token):
    """Проверки качества переносятся в каталог вместе с последними результатами."""
    os.environ.setdefault("ODS_CLICKHOUSE_HOST", "localhost")
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "etl"))
    import dq

    results = {(area, name): (status, value, rule) for area, name, status, value, rule in ch_rows(
        "select area, check_name, status, toString(value), rule from dds_dq_results "
        "where run_ts = (select max(run_ts) from dds_dq_results)")}

    created = 0
    for area, checks in dq.CHECKS.items():
        for check in checks:
            table = dq_table(area, check.name)
            fqn = f"{SCHEMA_FQN}.{table}"
            name = f"{area}_{check.name}"
            try:
                om("POST", "/dataQuality/testCases", token, {
                    "name": name,
                    "description": f"{check.name}: значение должно быть {check.op} "
                                   f"{check.limit}, уровень {check.severity}",
                    "entityLink": f"<#E::table::{fqn}>",
                    "testDefinition": "tableCustomSQLQuery",
                    "parameterValues": [
                        {"name": "sqlExpression", "value": check.sql},
                        {"name": "operator", "value": check.op},
                        {"name": "threshold", "value": str(check.limit)}]})
            except RuntimeError as e:
                if "409" not in str(e):
                    raise
            status, value, rule = results.get((area, check.name), ("", "", ""))
            if status:
                om("POST", f"/dataQuality/testCases/testCaseResults/{fqn}.{name}", token, {
                    "timestamp": now_ms(),
                    "testCaseStatus": {"ok": "Success", "fail": "Failed"}.get(status, "Aborted"),
                    "result": f"{value} при правиле {rule}",
                    "testResultValue": [{"name": check.name, "value": value}]})
            created += 1
    print(f"Качество данных: {created} проверок перенесено в каталог с последними результатами")


# ---------------------------------------------------------------- линейка

def create_lineage(token, entities, pipelines):
    for source, target, description, pipeline in LINEAGE:
        from_type, from_id = entities[source]
        to_type, to_id = entities[target]
        details = {"description": description}
        if pipeline in pipelines:
            details["pipeline"] = {"id": pipelines[pipeline], "type": "pipeline"}
        om("PUT", "/lineage", token, {"edge": {
            "fromEntity": {"id": from_id, "type": from_type},
            "toEntity": {"id": to_id, "type": to_type},
            "lineageDetails": details}})
    print(f"Линейка: {len(LINEAGE)} рёбер, у каждого указан процесс-исполнитель")


def main():
    token = om_login()
    air_token = airflow_token()
    structure, keys = table_structure()
    volume = table_volume()

    team_id = create_business(token)
    create_services(token)
    entities = create_containers(token, team_id)
    entities.update(create_tables(token, team_id, structure, keys, volume))
    pipelines = create_pipelines(token, team_id, air_token)
    create_lineage(token, entities, pipelines)
    create_quality(token)

    print(f"\nКаталог: http://localhost:8585/table/{SCHEMA_FQN}.dm_traffic_daily")
    print("Вход admin@open-metadata.org / admin")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as e:
        print(f"Ошибка: {e}")
        sys.exit(1)
