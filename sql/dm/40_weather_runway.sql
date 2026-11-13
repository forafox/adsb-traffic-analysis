-- Роза ветров и рабочая полоса.
-- Направление ветра в METAR — это откуда дует, а взлёт и посадка идут против ветра,
-- поэтому рабочей считается полоса, курс которой ближе всего к направлению ветра.
-- Курсы полос Кейптауна: 01/19 — это 010 и 190 градусов, 16/34 — это 160 и 340.
-- Для остальных аэропортов раскладка полос в проекте не хранится, поэтому поле пустое.
-- Боковая составляющая ветра — то, что ограничивает посадку: чем она больше,
-- тем менее удачен выбор полосы.

truncate table dm_weather_runway;

insert into dm_weather_runway
with
    [10., 190., 160., 340.] as runway_heads,
    ['01', '19', '16', '34'] as runway_names,
    obs as (
        select
            icao_code,
            wind_speed_kt,
            toUInt16(toInt32(round(wind_dir_deg / 45)) * 45 % 360) as sector_deg,
            arrayMap(h -> abs(((h - wind_dir_deg) + 540) % 360 - 180),
                     [10., 190., 160., 340.]) as diffs
        from dds_fact_weather final
        where wind_dir_deg is not null and wind_speed_kt is not null
    ),
    totals as (select icao_code, count() as n from obs group by icao_code)
select
    o.icao_code,
    o.sector_deg,
    arrayElement(['С', 'СВ', 'В', 'ЮВ', 'Ю', 'ЮЗ', 'З', 'СЗ'],
                 toInt32(o.sector_deg / 45) + 1) as wind_sector,
    toUInt32(count()) as observations,
    round(count() / any(t.n), 3) as share,
    round(avg(o.wind_speed_kt), 1) as avg_wind_kt,
    round(max(o.wind_speed_kt), 1) as max_wind_kt,
    if(o.icao_code = 'FACT',
       topK(1)(arrayElement(runway_names, indexOf(o.diffs, arrayMin(o.diffs))))[1],
       '') as preferred_runway,
    if(o.icao_code = 'FACT',
       round(avg(o.wind_speed_kt * sin(arrayMin(o.diffs) * pi() / 180)), 1),
       null) as avg_crosswind_kt
from obs as o
inner join totals as t on t.icao_code = o.icao_code
group by o.icao_code, o.sector_deg;
