-- Факт «наблюдение погоды»: одна запись на (станция, время наблюдения).
-- Сводки METAR в разных запусках перекрываются (каждый запрос берёт 12 часов истории),
-- поэтому дубли схлопываются, берётся значение из самого позднего снимка.
-- Значения вне физического диапазона заменяются на NULL. Ветер в узлах, давление в гПа,
-- видимость в милях (у METAR «6+» означает «6 и больше», плюс отбрасывается).

insert into dds_fact_weather
select
    w.w_icao,
    a.airport_key,
    w.w_ts,
    toYYYYMMDD(w.w_ts),
    w.wind_dir_c,
    w.wind_speed_c,
    w.vis_c,
    w.temp_c_c,
    w.alt_c,
    w.w_raw,
    toUInt64(toUnixTimestamp64Milli(now64(3)))
from (
    select
        icao as w_icao,
        parseDateTimeBestEffortOrZero(report_time, 'UTC') as w_ts,
        argMax(wind_dir, run_id) as w_dir,
        argMax(wind_speed, run_id) as w_speed,
        argMax(visibility, run_id) as w_vis,
        argMax(temp_c, run_id) as w_temp,
        argMax(altimeter, run_id) as w_alt,
        argMax(raw_ob, run_id) as w_raw,
        if(w_dir >= 0 and w_dir <= 360, w_dir, NULL) as wind_dir_c,
        if(w_speed >= 0 and w_speed <= 150, w_speed, NULL) as wind_speed_c,
        toFloat64OrNull(replaceAll(w_vis, '+', '')) as vis_c,
        if(w_temp >= -60 and w_temp <= 60, w_temp, NULL) as temp_c_c,
        if(w_alt >= 850 and w_alt <= 1100, w_alt, NULL) as alt_c
    from ods_metar
    where icao != '' and report_time != ''
    group by icao, w_ts
    having w_ts > toDateTime(0, 'UTC')
) as w
left join (select icao_code, airport_key from dds_dim_airport final where icao_code != '') as a
    on w.w_icao = a.icao_code;
