-- Справочные измерения, которые не зависят от потока данных.
-- Строки с ключом 0 (или пустым кодом) обозначают «неизвестно»: факт всегда ссылается на измерение.

insert into dds_dim_date
select
    toYYYYMMDD(d) as date_key,
    d as date,
    toYear(d) as year,
    toQuarter(d) as quarter,
    toMonth(d) as month,
    toDayOfMonth(d) as day,
    toDayOfWeek(d) as day_of_week,
    arrayElement(['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье'], toDayOfWeek(d)) as day_name,
    toUInt8(toDayOfWeek(d) >= 6) as is_weekend
from (select toDate('2026-01-01') + number as d from numbers(1500))
where toYYYYMMDD(d) not in (select date_key from dds_dim_date);

insert into dds_dim_source
select * from (
    select toUInt8(1) as source_key, 'adsblol' as source_name, 'adsb.lol: волонтёрская сеть, отдаёт регистрацию и тип самолёта' as description
    union all select 2, 'adsbfi', 'adsb.fi: своя сеть приёмников'
    union all select 3, 'opensky', 'OpenSky Network: хорошо видит малую авиацию, без регистрации и типа'
)
where source_key not in (select source_key from dds_dim_source);

insert into dds_dim_region
select * from (
    select 'cpt' as region_code, 'Кейптаун и аэропорт CPT, около 120 x 130 км' as region_name
    union all select 'za', 'вся ЮАР'
)
where region_code not in (select region_code from dds_dim_region);

insert into dds_dim_airline
select * from (select toUInt64(0) as airline_key, 'Неизвестно' as airline_name)
where airline_key not in (select airline_key from dds_dim_airline);

insert into dds_dim_airport
select * from (
    select toUInt64(0) as airport_key, 'Неизвестно' as airport_name, '' as icao_code
    union all select cityHash64('cape town'), 'Cape Town', 'FACT'
    union all select cityHash64('johannesburg'), 'Johannesburg', 'FAOR'
    union all select cityHash64('durban'), 'Durban', 'FALE'
    union all select cityHash64('port elizabeth'), 'Port Elizabeth', 'FAPE'
)
where airport_key not in (select airport_key from dds_dim_airport);

insert into dds_dim_aircraft_type
select * from (select '' as type_code, 'Неизвестно' as type_desc)
where type_code not in (select type_code from dds_dim_aircraft_type);

insert into dds_dim_aircraft
select * from (
    select toUInt64(0) as aircraft_sk, '' as icao24, '' as registration, '' as type_code, '' as operator,
           toDateTime(0, 'UTC') as valid_from, toDateTime('2100-01-01 00:00:00', 'UTC') as valid_to,
           toUInt8(1) as is_current, toUInt64(0) as row_version
)
where aircraft_sk not in (select aircraft_sk from dds_dim_aircraft);
