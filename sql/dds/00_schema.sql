-- Слой DDS в ClickHouse. Таблицы лежат в той же базе ods, префикс dds_ отделяет их от сырого слоя.
-- Все скрипты идемпотентны: повторный запуск не создаёт дублей.
--
-- Модель: созвездие фактов (fact_position, fact_aircraft_snapshot, fact_flight, fact_weather)
-- с общими измерениями. Самолёт оформлен снежинкой: dim_aircraft -> dim_aircraft_type.
-- ReplacingMergeTree заменяет строки с одинаковым ключом сортировки, поэтому читать
-- такие таблицы нужно с FINAL.

create table if not exists dds_dim_date (
    date_key UInt32,
    date Date,
    year UInt16,
    quarter UInt8,
    month UInt8,
    day UInt8,
    day_of_week UInt8,
    day_name String,
    is_weekend UInt8
) engine = ReplacingMergeTree order by date_key;

create table if not exists dds_dim_source (
    source_key UInt8,
    source_name String,
    description String
) engine = ReplacingMergeTree order by source_key;

create table if not exists dds_dim_region (
    region_code String,
    region_name String
) engine = ReplacingMergeTree order by region_code;

create table if not exists dds_dim_airline (
    airline_key UInt64,
    airline_name String
) engine = ReplacingMergeTree order by airline_key;

create table if not exists dds_dim_airport (
    airport_key UInt64,
    airport_name String,
    icao_code String
) engine = ReplacingMergeTree order by airport_key;

create table if not exists dds_dim_aircraft_type (
    type_code String,
    type_desc String
) engine = ReplacingMergeTree order by type_code;

-- SCD типа 2: каждая версия атрибутов самолёта живёт в своём интервале [valid_from, valid_to).
-- Строка текущей версии имеет valid_to = 2100-01-01 и is_current = 1.
-- Закрытие версии делается повторной вставкой строки с тем же ключом и новым row_version.
create table if not exists dds_dim_aircraft (
    aircraft_sk UInt64,
    icao24 String,
    registration String,
    type_code String,
    operator String,
    valid_from DateTime('UTC'),
    valid_to DateTime('UTC'),
    is_current UInt8,
    row_version UInt64
) engine = ReplacingMergeTree(row_version) order by (icao24, valid_from);

create table if not exists dds_fact_position (
    run_id String,
    snapshot_ts DateTime('UTC'),
    date_key UInt32,
    source_key UInt8,
    region_code String,
    icao24 String,
    aircraft_sk UInt64,
    callsign String,
    lat Float64,
    lon Float64,
    alt_m Nullable(Float64),
    on_ground UInt8,
    speed_ms Nullable(Float64),
    track Nullable(Float64)
) engine = ReplacingMergeTree
partition by toYYYYMM(snapshot_ts)
order by (run_id, source_key, region_code, icao24);

create table if not exists dds_fact_aircraft_snapshot (
    run_id String,
    snapshot_ts DateTime('UTC'),
    date_key UInt32,
    icao24 String,
    aircraft_sk UInt64,
    callsign String,
    lat Float64,
    lon Float64,
    alt_m Nullable(Float64),
    on_ground UInt8,
    speed_ms Nullable(Float64),
    track Nullable(Float64),
    best_source_key UInt8,
    sources_count UInt8,
    in_cpt UInt8
) engine = ReplacingMergeTree
partition by toYYYYMM(snapshot_ts)
order by (run_id, icao24);

create table if not exists dds_fact_flight (
    flight_key UInt64,
    flight_date Date,
    date_key UInt32,
    direction LowCardinality(String),
    flight_number String,
    airline_key UInt64,
    airport_key UInt64,
    status String,
    scheduled_ts DateTime('UTC'),
    actual_ts Nullable(DateTime('UTC')),
    delay_min Nullable(Int32),
    snapshots_seen UInt32,
    last_run_id String,
    row_version UInt64
) engine = ReplacingMergeTree(row_version)
partition by toYYYYMM(flight_date)
order by flight_key;

create table if not exists dds_fact_weather (
    icao_code String,
    airport_key UInt64,
    observed_ts DateTime('UTC'),
    date_key UInt32,
    wind_dir_deg Nullable(Float64),
    wind_speed_kt Nullable(Float64),
    visibility_sm Nullable(Float64),
    temp_c Nullable(Float64),
    altimeter_hpa Nullable(Float64),
    raw_ob String,
    row_version UInt64
) engine = ReplacingMergeTree(row_version)
partition by toYYYYMM(observed_ts)
order by (icao_code, observed_ts);

create table if not exists dds_dq_results (
    run_ts DateTime('UTC'),
    area String,
    check_name String,
    severity LowCardinality(String),
    status LowCardinality(String),
    value Float64,
    rule String
) engine = MergeTree order by (run_ts, area, check_name);
