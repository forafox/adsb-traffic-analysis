-- Авиакомпании и аэропорты из табло. SCD типа 1: название просто перезаписывается.
-- Ключ считается от названия в нижнем регистре, поэтому один и тот же объект из разных
-- снимков получает один и тот же ключ, а неизвестное значение получает ключ 0.
-- ICAO-код уже известных аэропортов (из 10_static.sql) сохраняется при перезаписи.

insert into dds_dim_airline
select cityHash64(lower(n)) as k, any(n) as nm
from (select trim(airline) as n from ods_flights)
where n != ''
group by k;

insert into dds_dim_airport
select b.k, b.nm, e.icao_code
from (
    select cityHash64(lower(n)) as k, any(n) as nm
    from (select trim(if(direction = 'arrivals', origin, destination)) as n from ods_flights)
    where n != ''
    group by k
) as b
left join (select airport_key, icao_code from dds_dim_airport final) as e on b.k = e.airport_key;
