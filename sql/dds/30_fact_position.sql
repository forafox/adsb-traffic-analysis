-- Факт «позиция борта»: очищенные записи ODS, по одной на (снимок, источник, регион, борт).
-- Чистка: пустые и невозможные координаты отбрасываются, некорректный icao24 отбрасывается,
-- дубли внутри снимка схлопываются, высота/скорость/курс вне физического диапазона
-- заменяются на NULL (позиция при этом остаётся).
-- Загружаются только снимки, которых ещё нет в факте.

insert into dds_fact_position
select
    c.run_id,
    c.snapshot_ts,
    toYYYYMMDD(c.snapshot_ts) as date_key,
    s.source_key,
    c.region,
    c.icao,
    a.aircraft_sk,
    c.cs,
    c.lat,
    c.lon,
    c.alt_clean,
    c.on_ground,
    c.speed_clean,
    c.track_clean
from (
    select
        run_id, source, region,
        lower(icao24) as icao,
        parseDateTimeBestEffortOrZero(fetched_at, 'UTC') as snapshot_ts,
        trim(callsign) as cs,
        lat, lon,
        if(alt_m >= -500 and alt_m <= 20000, alt_m, NULL) as alt_clean,
        on_ground,
        if(speed_ms >= 0 and speed_ms <= 400, speed_ms, NULL) as speed_clean,
        if(track >= 0 and track <= 360, track, NULL) as track_clean
    from ods_positions
    where run_id not in (select distinct run_id from dds_fact_position)
      and fetched_at != ''
      and lat is not null and lon is not null
      and lat >= -90 and lat <= 90 and lon >= -180 and lon <= 180
      and length(icao24) = 6 and not match(lower(icao24), '[^0-9a-f]')
    limit 1 by run_id, source, region, icao
) as c
asof left join (select icao24, valid_from, aircraft_sk from dds_dim_aircraft final) as a
    on c.icao = a.icao24 and c.snapshot_ts >= a.valid_from
inner join (select source_key, source_name from dds_dim_source final) as s
    on c.source = s.source_name;
