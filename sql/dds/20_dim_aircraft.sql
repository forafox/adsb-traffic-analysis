-- Измерение «самолёт» (SCD2) и справочник типов.
-- Регистрацию, тип и оператора отдают только adsb.lol и adsb.fi, OpenSky их не знает.
-- История версий пересобирается из всего ODS: версия появляется, когда атрибуты борта
-- изменились относительно предыдущего снимка. Ключ (icao24, valid_from) не меняется,
-- поэтому повторный запуск заменяет строки, а не плодит их.
-- Первая версия борта открыта в прошлое (valid_from = 1970-01-01), чтобы факты, снятые
-- до первого появления атрибутов, тоже находили версию.

insert into dds_dim_aircraft_type
select typ as type_code, argMax(descr, (descr != '', obs_ts)) as type_desc
from (
    select JSONExtractString(raw, 't') as typ,
           JSONExtractString(raw, 'desc') as descr,
           parseDateTimeBestEffortOrZero(fetched_at, 'UTC') as obs_ts
    from ods_positions
    where source in ('adsblol', 'adsbfi')
)
where typ != ''
group by typ;

insert into dds_dim_aircraft
with
obs as (
    select
        icao24,
        min(parseDateTimeBestEffortOrZero(fetched_at, 'UTC')) as obs_ts,
        argMax(JSONExtractString(raw, 'r'), (JSONExtractString(raw, 'r') != '', source = 'adsblol')) as reg,
        argMax(JSONExtractString(raw, 't'), (JSONExtractString(raw, 't') != '', source = 'adsblol')) as typ,
        argMax(JSONExtractString(raw, 'ownOp'), (JSONExtractString(raw, 'ownOp') != '', source = 'adsblol')) as op
    from ods_positions
    where source in ('adsblol', 'adsbfi') and icao24 != '' and fetched_at != ''
    group by icao24, run_id
    having reg != '' or typ != ''
),
chg as (
    select
        icao24, obs_ts, reg, typ, op,
        row_number() over w as rn,
        lagInFrame(reg) over w as p_reg,
        lagInFrame(typ) over w as p_typ,
        lagInFrame(op) over w as p_op
    from obs
    window w as (partition by icao24 order by obs_ts rows between unbounded preceding and unbounded following)
),
ver as (
    select
        icao24, reg, typ, op,
        if(rn = 1, toDateTime(0, 'UTC'), obs_ts) as valid_from
    from chg
    where rn = 1 or reg != p_reg or typ != p_typ or op != p_op
)
select
    cityHash64(icao24, toString(valid_from)) as aircraft_sk,
    icao24,
    reg as registration,
    typ as type_code,
    op as operator,
    valid_from,
    valid_to,
    toUInt8(valid_to = toDateTime('2100-01-01 00:00:00', 'UTC')) as is_current,
    toUInt64(toUnixTimestamp64Milli(now64(3))) as row_version
from (
    select
        icao24, reg, typ, op, valid_from,
        leadInFrame(valid_from, 1, toDateTime('2100-01-01 00:00:00', 'UTC')) over w2 as valid_to
    from ver
    window w2 as (partition by icao24 order by valid_from rows between unbounded preceding and unbounded following)
);
