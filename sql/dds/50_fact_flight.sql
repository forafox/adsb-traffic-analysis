-- Факт «рейс»: последнее состояние каждого рейса из всех снимков табло.
-- Рейс определяется (дата, направление, номер, время по расписанию). Табло снималось
-- несколько раз, поэтому берётся состояние из самого позднего снимка (argMax по run_id).
-- Время на табло местное (UTC+2), в DDS оно переводится в UTC.
-- Задержка = фактическое время минус расписание в минутах, считается только для вылетевших
-- и севших рейсов. Переход через полночь поправлен (разница больше 12 часов сдвигается
-- на сутки), задержки больше 4 часов считаются ошибкой табло и остаются NULL.

insert into dds_fact_flight
with
src as (
    select
        flight_date,
        direction,
        trim(flight_number) as f_no,
        schedule_time,
        argMax(trim(airline), run_id) as f_airline,
        argMax(status, run_id) as f_status,
        argMax(updated_time, run_id) as f_updated,
        argMax(trim(if(direction = 'arrivals', origin, destination)), run_id) as f_counterpart,
        count() as f_snapshots,
        max(run_id) as f_last_run
    from ods_flights
    where trim(flight_number) != ''
      and match(schedule_time, '^[0-9][0-9]:[0-9][0-9]$')
      and toDateOrNull(flight_date) is not null
      and direction in ('departures', 'arrivals')
    group by flight_date, direction, f_no, schedule_time
),
calc as (
    select
        *,
        toInt32OrNull(substring(schedule_time, 1, 2)) * 60 + toInt32OrNull(substring(schedule_time, 4, 2)) as sched_min,
        if(f_status in ('Departed', 'Landed') and match(f_updated, '^[0-9][0-9]:[0-9][0-9]$'),
           toInt32OrNull(substring(f_updated, 1, 2)) * 60 + toInt32OrNull(substring(f_updated, 4, 2)),
           NULL) as act_min
    from src
),
delay as (
    select
        *,
        act_min - sched_min as raw_delay,
        multiIf(raw_delay > 720, raw_delay - 1440, raw_delay < -720, raw_delay + 1440, raw_delay) as wrapped_delay,
        if(abs(wrapped_delay) <= 240, wrapped_delay, NULL) as delay_clean,
        toDateTime(concat(flight_date, ' ', schedule_time, ':00'), 'Africa/Johannesburg') as sched_local
    from calc
)
select
    cityHash64(flight_date, direction, f_no, schedule_time) as flight_key,
    toDate(flight_date) as flight_date_d,
    toYYYYMMDD(toDate(flight_date)) as date_key,
    direction,
    f_no,
    if(f_airline = '', toUInt64(0), cityHash64(lower(f_airline))) as airline_key,
    if(f_counterpart = '', toUInt64(0), cityHash64(lower(f_counterpart))) as airport_key,
    f_status,
    toTimeZone(sched_local, 'UTC') as scheduled_ts,
    if(delay_clean is null, NULL, toTimeZone(addMinutes(sched_local, delay_clean), 'UTC')) as actual_ts,
    delay_clean,
    toUInt32(f_snapshots) as snapshots_seen,
    f_last_run,
    toUInt64(toUnixTimestamp64Milli(now64(3))) as row_version
from delay;
