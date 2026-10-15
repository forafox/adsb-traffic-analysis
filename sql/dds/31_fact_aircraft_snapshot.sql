-- Слияние источников: один борт в одном снимке превращается в одну строку.
-- Координаты и скорость берутся у источника с наименьшим source_key (adsb.lol, затем adsb.fi,
-- затем OpenSky), sources_count показывает, сколькими сетями борт виден,
-- in_cpt отмечает попадание в зону Кейптауна (зона cpt входит в зону za).
-- Псевдонимы отличаются от имён колонок, чтобы не путать агрегат с исходным столбцом.

insert into dds_fact_aircraft_snapshot
select
    run_id,
    min(snapshot_ts) as m_ts,
    toYYYYMMDD(min(snapshot_ts)) as m_date_key,
    icao24,
    any(aircraft_sk) as m_aircraft_sk,
    argMax(callsign, callsign != '') as m_callsign,
    argMin(lat, source_key) as m_lat,
    argMin(lon, source_key) as m_lon,
    argMin(alt_m, source_key) as m_alt,
    argMin(on_ground, source_key) as m_ground,
    argMin(speed_ms, source_key) as m_speed,
    argMin(track, source_key) as m_track,
    min(source_key) as m_best_source,
    toUInt8(uniqExact(source_key)) as m_sources,
    toUInt8(countIf(region_code = 'cpt') > 0) as m_in_cpt
from dds_fact_position final
where run_id not in (select distinct run_id from dds_fact_aircraft_snapshot)
group by run_id, icao24;
