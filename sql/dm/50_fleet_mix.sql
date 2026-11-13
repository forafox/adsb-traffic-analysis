-- Состав наблюдаемого парка по типам самолётов.
-- Считается по слитому факту, где борт в снимке представлен одной записью,
-- иначе типы, которые видят все три сети, получили бы тройной вес.
-- Категория определяется по средней высоте наблюдений: магистральные борта идут
-- эшелонами выше трёх километров, местные рейсы и малая авиация — ниже.
-- Это эвристика по наблюдениям, а не справочник типов воздушных судов.

truncate table dm_fleet_mix;

insert into dm_fleet_mix
with
    aircraft as (
        select
            s.icao24,
            any(ifNull(a.type_code, '')) as type_code,
            count() as observations,
            avg(s.alt_m) as avg_alt_m,
            max(s.alt_m) as max_alt_m,
            avg(s.speed_ms) as avg_speed_ms
        from (select * from dds_fact_aircraft_snapshot final) as s
        left join (select * from dds_dim_aircraft final) as a on a.aircraft_sk = s.aircraft_sk
        group by s.icao24
    ),
    total as (select uniqExact(icao24) as n from aircraft)
select
    c.type_code,
    ifNull(any(t.type_desc), 'Неизвестно') as type_desc,
    toUInt32(count()) as aircraft,
    toUInt32(sum(c.observations)) as observations,
    round(avg(c.avg_alt_m), 0) as avg_alt_m,
    round(max(c.max_alt_m), 0) as max_alt_m,
    round(avg(c.avg_speed_ms), 1) as avg_speed_ms,
    round(count() / (select n from total), 3) as aircraft_share,
    multiIf(avg(c.avg_alt_m) is null, 'неизвестно',
            avg(c.avg_alt_m) >= 3000, 'магистральные',
            'малая авиация и местные') as category
from aircraft as c
left join (select * from dds_dim_aircraft_type final) as t on t.type_code = c.type_code
group by c.type_code;
