"""Проверки качества данных слоя DDS.

Каждая проверка это SQL, возвращающий одно число, и правило для него: не больше или
не меньше порога. Результаты пишутся в таблицу dds_dq_results, чтобы историю качества
можно было смотреть запросом. Серьёзность error валит DAG, warn только отмечается.
"""

from collections import namedtuple
from datetime import datetime, timezone

import dds

Check = namedtuple("Check", "name sql op limit severity")


def missing_ref(name, fact, col, dim, key):
    return Check(name, f"select count() from {fact} final where {col} not in "
                       f"(select {key} from {dim} final)", "<=", 0, "error")


CHECKS = {
    "positions": [
        # объём до и после трансформации
        Check("rows_ods_vs_dds",
              "select if(o = 0, 1, d / o) from (select "
              "(select uniqExact(run_id, source, region, lower(icao24)) from ods_positions) as o, "
              "(select count() from dds_fact_position final) as d)", ">=", 0.9, "error"),
        Check("snapshot_covers_positions",
              "select abs(a - b) from (select "
              "(select uniqExact(run_id, icao24) from dds_fact_position final) as a, "
              "(select count() from dds_fact_aircraft_snapshot final) as b)", "<=", 0, "error"),
        Check("duplicate_positions",
              "select count() - uniqExact(run_id, source_key, region_code, icao24) "
              "from dds_fact_position final", "<=", 0, "error"),
        Check("bad_coordinates",
              "select count() from dds_fact_position final where lat < -90 or lat > 90 "
              "or lon < -180 or lon > 180", "<=", 0, "error"),
        Check("altitude_out_of_range",
              "select count() from dds_fact_position final where alt_m < -500 or alt_m > 20000",
              "<=", 0, "error"),
        Check("speed_out_of_range",
              "select count() from dds_fact_position final where speed_ms < 0 or speed_ms > 400",
              "<=", 0, "error"),
        Check("bad_icao24",
              "select count() from dds_fact_position final where length(icao24) != 6",
              "<=", 0, "error"),
        Check("empty_callsign_share",
              "select if(count() = 0, 0, countIf(callsign = '') / count()) "
              "from dds_fact_position final", "<=", 0.5, "warn"),
        Check("unknown_aircraft_share",
              "select if(count() = 0, 0, countIf(aircraft_sk = 0) / count()) "
              "from dds_fact_position final", "<=", 0.6, "warn"),
    ],
    "flights": [
        Check("rows_ods_vs_dds",
              "select if(o = 0, 1, d / o) from (select "
              "(select uniqExact(flight_date, direction, trim(flight_number), schedule_time) "
              "from ods_flights where trim(flight_number) != '' "
              "and match(schedule_time, '^[0-9][0-9]:[0-9][0-9]$') "
              "and direction in ('departures', 'arrivals')) as o, "
              "(select count() from dds_fact_flight final) as d)", ">=", 0.99, "error"),
        Check("duplicate_flights",
              "select count() - uniqExact(flight_key) from dds_fact_flight final",
              "<=", 0, "error"),
        Check("empty_flight_number",
              "select count() from dds_fact_flight final where flight_number = ''",
              "<=", 0, "error"),
        Check("delay_out_of_range",
              "select count() from dds_fact_flight final where abs(delay_min) > 240",
              "<=", 0, "error"),
        # неполный сбор: у дня должны быть и вылеты, и прилёты
        Check("both_directions_per_day",
              "select count() from (select flight_date from dds_fact_flight final "
              "group by flight_date having uniqExact(direction) < 2)", "<=", 0, "error"),
        Check("unknown_airline_share",
              "select if(count() = 0, 0, countIf(airline_key = 0) / count()) "
              "from dds_fact_flight final", "<=", 0.05, "warn"),
        Check("unknown_airport_share",
              "select if(count() = 0, 0, countIf(airport_key = 0) / count()) "
              "from dds_fact_flight final", "<=", 0.05, "warn"),
    ],
    "weather": [
        Check("rows_ods_vs_dds",
              "select if(o = 0, 1, d / o) from (select "
              "(select uniqExact(icao, report_time) from ods_metar "
              "where icao != '' and report_time != '') as o, "
              "(select count() from dds_fact_weather final) as d)", ">=", 0.99, "error"),
        Check("duplicate_observations",
              "select count() - uniqExact(icao_code, observed_ts) from dds_fact_weather final",
              "<=", 0, "error"),
        Check("wind_out_of_range",
              "select count() from dds_fact_weather final where wind_speed_kt < 0 "
              "or wind_speed_kt > 150 or wind_dir_deg < 0 or wind_dir_deg > 360",
              "<=", 0, "error"),
        Check("temperature_out_of_range",
              "select count() from dds_fact_weather final where temp_c < -60 or temp_c > 60",
              "<=", 0, "error"),
        Check("pressure_out_of_range",
              "select count() from dds_fact_weather final "
              "where altimeter_hpa < 850 or altimeter_hpa > 1100", "<=", 0, "error"),
        Check("unknown_station_share",
              "select if(count() = 0, 0, countIf(airport_key = 0) / count()) "
              "from dds_fact_weather final", "<=", 0.0, "warn"),
    ],
    "integrity": [
        missing_ref("position_source_in_dim", "dds_fact_position", "source_key",
                    "dds_dim_source", "source_key"),
        missing_ref("position_region_in_dim", "dds_fact_position", "region_code",
                    "dds_dim_region", "region_code"),
        missing_ref("position_date_in_dim", "dds_fact_position", "date_key",
                    "dds_dim_date", "date_key"),
        missing_ref("position_aircraft_in_dim", "dds_fact_position", "aircraft_sk",
                    "dds_dim_aircraft", "aircraft_sk"),
        missing_ref("flight_airline_in_dim", "dds_fact_flight", "airline_key",
                    "dds_dim_airline", "airline_key"),
        missing_ref("flight_airport_in_dim", "dds_fact_flight", "airport_key",
                    "dds_dim_airport", "airport_key"),
        missing_ref("flight_date_in_dim", "dds_fact_flight", "date_key",
                    "dds_dim_date", "date_key"),
        missing_ref("weather_airport_in_dim", "dds_fact_weather", "airport_key",
                    "dds_dim_airport", "airport_key"),
        missing_ref("weather_date_in_dim", "dds_fact_weather", "date_key",
                    "dds_dim_date", "date_key"),
        missing_ref("aircraft_type_in_dim", "dds_dim_aircraft", "type_code",
                    "dds_dim_aircraft_type", "type_code"),
        # SCD2: у каждого борта ровно одна текущая версия
        Check("scd2_one_current_version",
              "select count() from (select icao24 from dds_dim_aircraft final "
              "group by icao24 having sum(is_current) != 1)", "<=", 0, "error"),
        # SCD2: версии идут встык, без дыр и пересечений
        Check("scd2_no_gaps_or_overlaps",
              "select count() from (select valid_to, leadInFrame(valid_from, 1, valid_to) "
              "over (partition by icao24 order by valid_from "
              "rows between unbounded preceding and unbounded following) as next_from "
              "from dds_dim_aircraft final) where valid_to != next_from", "<=", 0, "error"),
    ],
}


def evaluate(check, value):
    ok = value <= check.limit if check.op == "<=" else value >= check.limit
    if ok:
        return "ok"
    return "fail" if check.severity == "error" else "warn"


def run(area):
    """Выполняет проверки области, пишет результаты в dds_dq_results, возвращает число провалов."""
    ch = dds.client()
    run_ts = datetime.now(timezone.utc).replace(microsecond=0)
    rows, failed = [], 0
    for check in CHECKS[area]:
        value = float(ch.query(check.sql).result_rows[0][0] or 0)
        status = evaluate(check, value)
        failed += status == "fail"
        rows.append([run_ts, area, check.name, check.severity, status, value,
                     f"{check.op} {check.limit}"])
        print(f"[{status:4s}] {area}.{check.name} = {value:g} (нужно {check.op} {check.limit})")
    ch.insert("dds_dq_results", rows, column_names=[
        "run_ts", "area", "check_name", "severity", "status", "value", "rule"])
    return failed
