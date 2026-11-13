-- Слой витрин в ClickHouse, префикс dm_ отделяет их от детального слоя.
-- Каждая витрина отвечает на один аналитический вопрос и рассчитана на прямое чтение
-- из Superset без соединений: все нужные названия уже подставлены из измерений.
--
-- Витрины пересобираются целиком (truncate + insert), поэтому движок обычный MergeTree:
-- схлопывать нечего, а устаревшие строки не остаются.

-- Вопрос: когда аэропорт загружен и в какие часы рейсы задерживаются сильнее.
create table if not exists dm_airport_load_hourly (
    hour_local UInt8,
    direction LowCardinality(String),
    flights UInt32,
    days UInt16,
    flights_per_day Float64,
    avg_delay_min Nullable(Float64),
    late_share Float64
) engine = MergeTree order by (direction, hour_local);

-- Вопрос: какие авиакомпании летают пунктуально, а какие стабильно опаздывают.
create table if not exists dm_airline_punctuality (
    airline_name String,
    direction LowCardinality(String),
    flights UInt32,
    delays_known UInt32,
    avg_delay_min Nullable(Float64),
    median_delay_min Nullable(Float64),
    p90_delay_min Nullable(Float64),
    late_share Float64,
    early_share Float64,
    worst_delay_min Int32
) engine = MergeTree order by (airline_name, direction);

-- Вопрос: что даёт каждая сеть приёмников и оправдан ли сбор из трёх источников.
create table if not exists dm_source_coverage (
    source_name String,
    region_code String,
    snapshots UInt32,
    observations UInt32,
    aircraft_seen UInt32,
    avg_per_snapshot Float64,
    avg_alt_m Nullable(Float64),
    exclusive_observations UInt32,
    exclusive_share Float64
) engine = MergeTree order by (source_name, region_code);

-- Вопрос: откуда дует ветер в Кейптауне и какая полоса при этом рабочая.
create table if not exists dm_weather_runway (
    icao_code String,
    sector_deg UInt16,
    wind_sector LowCardinality(String),
    observations UInt32,
    share Float64,
    avg_wind_kt Float64,
    max_wind_kt Float64,
    preferred_runway String,
    avg_crosswind_kt Nullable(Float64)
) engine = MergeTree order by (icao_code, sector_deg);

-- Вопрос: какие типы бортов наблюдаются и велика ли доля малой авиации.
create table if not exists dm_fleet_mix (
    type_code String,
    type_desc String,
    aircraft UInt32,
    observations UInt32,
    avg_alt_m Nullable(Float64),
    max_alt_m Nullable(Float64),
    avg_speed_ms Nullable(Float64),
    aircraft_share Float64,
    category LowCardinality(String)
) engine = MergeTree order by type_code;

-- Вопрос: как меняется трафик день ото дня и связаны ли задержки с ветром.
create table if not exists dm_traffic_daily (
    date Date,
    day_name String,
    is_weekend UInt8,
    departures UInt32,
    arrivals UInt32,
    avg_delay_min Nullable(Float64),
    late_share Float64,
    aircraft_seen UInt32,
    observations UInt32,
    snapshots UInt32,
    avg_wind_kt Nullable(Float64),
    max_wind_kt Nullable(Float64)
) engine = MergeTree order by date;
