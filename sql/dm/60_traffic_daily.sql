-- Суточная динамика: рейсы, наблюдаемые борта и ветер в один ряд по датам.
-- Витрина сводит три факта, поэтому дни, когда собирались только позиции или только
-- табло, видны как пропуски — это честная картина сбора, а не ошибка.
-- Названия дней и признак выходного берутся из измерения дат, а не вычисляются заново.

truncate table dm_traffic_daily;

insert into dm_traffic_daily
with
    flights as (
        select
            flight_date as date,
            countIf(direction = 'departures') as departures,
            countIf(direction = 'arrivals') as arrivals,
            avg(delay_min) as avg_delay_min,
            ifNull(countIf(delay_min > 15) / nullIf(countIf(delay_min is not null), 0), 0)
                as late_share
        from dds_fact_flight final
        group by date
    ),
    traffic as (
        select
            toDate(snapshot_ts) as date,
            uniqExact(icao24) as aircraft_seen,
            count() as observations,
            uniqExact(run_id) as snapshots
        from dds_fact_aircraft_snapshot final
        group by date
    ),
    weather as (
        select
            toDate(observed_ts) as date,
            avg(wind_speed_kt) as avg_wind_kt,
            max(wind_speed_kt) as max_wind_kt
        from dds_fact_weather final
        where icao_code = 'FACT'
        group by date
    ),
    days as (
        select distinct date from (
            select date from flights
            union all select date from traffic
            union all select date from weather
        )
    )
select
    d.date,
    ifNull(c.day_name, '') as day_name,
    toUInt8(ifNull(c.is_weekend, 0)) as is_weekend,
    toUInt32(ifNull(f.departures, 0)) as departures,
    toUInt32(ifNull(f.arrivals, 0)) as arrivals,
    round(f.avg_delay_min, 1) as avg_delay_min,
    round(ifNull(f.late_share, 0), 3) as late_share,
    toUInt32(ifNull(t.aircraft_seen, 0)) as aircraft_seen,
    toUInt32(ifNull(t.observations, 0)) as observations,
    toUInt32(ifNull(t.snapshots, 0)) as snapshots,
    round(w.avg_wind_kt, 1) as avg_wind_kt,
    round(w.max_wind_kt, 1) as max_wind_kt
from days as d
left join flights as f on f.date = d.date
left join traffic as t on t.date = d.date
left join weather as w on w.date = d.date
left join (select date, day_name, is_weekend from dds_dim_date final) as c on c.date = d.date;
