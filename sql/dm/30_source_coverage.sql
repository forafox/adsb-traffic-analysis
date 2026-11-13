-- Покрытие источников ADS-B.
-- Эксклюзивным считается наблюдение, в котором борт в этом снимке и этом регионе
-- видела ровно одна сеть: это и есть ответ на вопрос, зачем собирать из трёх источников
-- вместо одного. Средняя высота показывает характер покрытия: сеть, которая ловит
-- низколетящую малую авиацию, даёт заметно меньшее среднее.

truncate table dm_source_coverage;

insert into dm_source_coverage
with observed as (
    select
        run_id, region_code, icao24,
        uniqExact(source_key) as sources,
        min(source_key) as only_source
    from dds_fact_position final
    group by run_id, region_code, icao24
),
exclusive as (
    select only_source as source_key, region_code, count() as observations
    from observed
    where sources = 1
    group by only_source, region_code
)
select
    s.source_name,
    p.region_code,
    toUInt32(uniqExact(p.run_id)) as snapshots,
    toUInt32(count()) as observations,
    toUInt32(uniqExact(p.icao24)) as aircraft_seen,
    round(count() / uniqExact(p.run_id), 1) as avg_per_snapshot,
    round(avg(p.alt_m), 0) as avg_alt_m,
    toUInt32(any(ifNull(e.observations, 0))) as exclusive_observations,
    round(any(ifNull(e.observations, 0)) / count(), 3) as exclusive_share
from (select * from dds_fact_position final) as p
inner join (select * from dds_dim_source final) as s on s.source_key = p.source_key
left join exclusive as e on e.source_key = p.source_key and e.region_code = p.region_code
group by s.source_name, p.region_code;
