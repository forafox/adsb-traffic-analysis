-- Пунктуальность авиакомпаний.
-- Задержка известна только у вылетевших и севших рейсов, поэтому доли считаются
-- не от всех рейсов, а от тех, у кого есть фактическое время.
-- Опоздавшим считается рейс с задержкой больше 15 минут — порог, принятый в отрасли.
-- Медиана и 90-й перцентиль показывают то, чего не видно по среднему: редкие
-- сильные опоздания сдвигают среднее, а типичный рейс при этом уходит вовремя.

truncate table dm_airline_punctuality;

insert into dm_airline_punctuality
select
    a.airline_name,
    f.direction,
    toUInt32(count()) as flights,
    toUInt32(countIf(f.delay_min is not null)) as delays_known,
    round(avg(f.delay_min), 1) as avg_delay_min,
    round(quantileExact(0.5)(f.delay_min), 1) as median_delay_min,
    round(quantileExact(0.9)(f.delay_min), 1) as p90_delay_min,
    round(ifNull(countIf(f.delay_min > 15) / nullIf(countIf(f.delay_min is not null), 0), 0), 3)
        as late_share,
    round(ifNull(countIf(f.delay_min < 0) / nullIf(countIf(f.delay_min is not null), 0), 0), 3)
        as early_share,
    toInt32(ifNull(max(f.delay_min), 0)) as worst_delay_min
from (select * from dds_fact_flight final) as f
inner join (select * from dds_dim_airline final) as a on a.airline_key = f.airline_key
group by a.airline_name, f.direction;
