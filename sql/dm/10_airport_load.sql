-- Загрузка аэропорта по часам суток.
-- Время по расписанию хранится в UTC, здесь переводится в местное (UTC+2):
-- профиль суток имеет смысл только в местном времени.
-- Число рейсов в день считается на все дни наблюдения, а не на дни, когда был рейс
-- в этот час, иначе редкие ночные часы выглядели бы такими же загруженными, как утренние.

truncate table dm_airport_load_hourly;

insert into dm_airport_load_hourly
select
    toUInt8(toHour(toTimeZone(scheduled_ts, 'Africa/Johannesburg'))) as hour_local,
    direction,
    toUInt32(count()) as flights,
    toUInt16((select uniqExact(flight_date) from dds_fact_flight final)) as days,
    round(count() / days, 2) as flights_per_day,
    round(avg(delay_min), 1) as avg_delay_min,
    round(ifNull(countIf(delay_min > 15) / nullIf(countIf(delay_min is not null), 0), 0), 3)
        as late_share
from dds_fact_flight final
group by hour_local, direction;
