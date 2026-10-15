# ЛР2. Нормализация данных и Data Quality

Тема: анализ воздушного движения по открытым ADS-B данным, регион — Кейптаун (ЮАР).

Из сырого слоя ODS (ЛР1) строится слой детальных данных DDS: очистка, приведение типов,
обогащение, слияние источников, медленно меняющиеся измерения, зависимости между DAGами
и отдельный DAG проверки качества.

## 1. Где строится DDS

DDS лежит в **ClickHouse**, в той же базе `ods`, таблицы с префиксом `dds_`. Это согласуется
с выводом ЛР1: ClickHouse выбран основным хранилищем (см. «Вывод» в [REPORT.md](REPORT.md)),
позиции бортов — главный и самый быстрорастущий набор.

Что даёт ClickHouse и как решены его ограничения:

| Особенность | Как учтено |
|---|---|
| нет дешёвого `UPDATE` | SCD2 и дедупликация сделаны на `ReplacingMergeTree`: изменение записывается как вставка строки с тем же ключом и новым `row_version` |
| нет внешних ключей | целостность проверяется DAGом качества (проверки `*_in_dim`) |
| дубли схлопываются только при слиянии кусков | факты читаются с `FINAL` |
| нет суррогатных ключей-последовательностей | ключи детерминированные: `cityHash64` от естественного ключа, поэтому повторный запуск даёт те же ключи |

> В ЛР1 была запись, что слой обработанных данных будет построен на PostgreSQL. Здесь
> выбран ClickHouse, формулировку в отчёте ЛР1 стоит привести в соответствие.

## 2. Структура хранилища

Выбрана **схема «созвездие» (несколько звёзд с общими измерениями)** с одной «снежинкой».

Звезда — это один факт, окружённый измерениями. У нас четыре разных процесса (позиции,
слитые позиции, рейсы, погода), поэтому четыре факта, которые делят общие измерения
(дата, аэропорт, источник). Снежинка — измерение, развёрнутое в несколько таблиц: самолёт
вынесен в `dim_aircraft` и `dim_aircraft_type`. Это сделано ради 3НФ: описание типа
(«BOEING 737-800») зависит от кода типа, а не от конкретного борта, и в одной таблице
оно бы дублировалось в каждой строке борта.

### Модель

```mermaid
erDiagram
    dim_date ||--o{ fact_position : date_key
    dim_date ||--o{ fact_aircraft_snapshot : date_key
    dim_date ||--o{ fact_flight : date_key
    dim_date ||--o{ fact_weather : date_key
    dim_source ||--o{ fact_position : source_key
    dim_region ||--o{ fact_position : region_code
    dim_aircraft ||--o{ fact_position : aircraft_sk
    dim_aircraft ||--o{ fact_aircraft_snapshot : aircraft_sk
    dim_aircraft_type ||--o{ dim_aircraft : type_code
    dim_airline ||--o{ fact_flight : airline_key
    dim_airport ||--o{ fact_flight : airport_key
    dim_airport ||--o{ fact_weather : airport_key

    fact_position {
        string run_id PK
        uint8 source_key PK
        string region_code PK
        string icao24 PK
        datetime snapshot_ts
        uint64 aircraft_sk FK
        float lat
        float lon
        float alt_m
        float speed_ms
        float track
        uint8 on_ground
    }
    fact_aircraft_snapshot {
        string run_id PK
        string icao24 PK
        datetime snapshot_ts
        uint64 aircraft_sk FK
        uint8 best_source_key
        uint8 sources_count
        uint8 in_cpt
    }
    fact_flight {
        uint64 flight_key PK
        string direction
        string flight_number
        uint64 airline_key FK
        uint64 airport_key FK
        string status
        datetime scheduled_ts
        datetime actual_ts
        int delay_min
    }
    fact_weather {
        string icao_code PK
        datetime observed_ts PK
        uint64 airport_key FK
        float wind_dir_deg
        float wind_speed_kt
        float visibility_sm
        float temp_c
        float altimeter_hpa
    }
    dim_aircraft {
        uint64 aircraft_sk PK
        string icao24
        string registration
        string type_code FK
        string operator
        datetime valid_from
        datetime valid_to
        uint8 is_current
    }
    dim_aircraft_type {
        string type_code PK
        string type_desc
    }
    dim_airline {
        uint64 airline_key PK
        string airline_name
    }
    dim_airport {
        uint64 airport_key PK
        string airport_name
        string icao_code
    }
    dim_source {
        uint8 source_key PK
        string source_name
    }
    dim_region {
        string region_code PK
        string region_name
    }
    dim_date {
        uint32 date_key PK
        date date
    }
```

Диаграмма написана в Mermaid и отображается прямо на GitHub. Для презентации модель
можно нарисовать в draw.io, dbdiagram.io или в ER-редакторе DBeaver (он строит диаграмму
по существующим таблицам).

### Таблицы

| Таблица | Тип | Грануляр­ность / ключ | Откуда |
|---|---|---|---|
| `dds_fact_position` | факт | позиция борта в снимке: (run_id, источник, регион, icao24) | `ods_positions` |
| `dds_fact_aircraft_snapshot` | факт (агрегат) | борт в снимке: (run_id, icao24), источники слиты | `dds_fact_position` |
| `dds_fact_flight` | факт | рейс: (дата, направление, номер, время по расписанию) | `ods_flights` |
| `dds_fact_weather` | факт | наблюдение: (станция, время) | `ods_metar` |
| `dds_dim_aircraft` | измерение, **SCD2** | версия атрибутов борта | `ods_positions.raw` |
| `dds_dim_aircraft_type` | измерение, SCD1 | код типа | `ods_positions.raw` |
| `dds_dim_airline` | измерение, SCD1 | авиакомпания | `ods_flights` |
| `dds_dim_airport` | измерение, SCD1 | аэропорт | `ods_flights`, справочник |
| `dds_dim_source` | измерение | источник ADS-B | справочник |
| `dds_dim_region` | измерение | регион опроса (cpt, za) | справочник |
| `dds_dim_date` | измерение | день | генерируется |
| `dds_dq_results` | служебная | результат проверки | DAG качества |

### Приведение к 3НФ

- Атрибуты авиакомпании, аэропорта, источника и региона не повторяются в фактах, факты хранят
  только ключи и измеряемые значения.
- Транзитивная зависимость «борт → тип → описание типа» разорвана выделением
  `dim_aircraft_type`.
- Составное направление «откуда / куда» в табло приведено к одному полю `airport_key`:
  для вылета это пункт назначения, для прилёта — пункт отправления, второй конец всегда
  Кейптаун. Направление хранится отдельно в `direction`.
- Время по расписанию и фактическое время хранятся как `DateTime` в UTC, а не строками
  «05:45» без даты. Фактическое время восстанавливается как расписание плюс задержка.
- Неизвестные значения не хранятся как NULL в ключах: для них есть строки-«неизвестно»
  (ключ 0 или пустой код), поэтому ссылка из факта всегда валидна.

## 3. Очистка, обогащение, агрегация

Вся логика в SQL-скриптах [sql/dds](sql/dds), Python только запускает их из Airflow.

**Очистка** (`30_fact_position.sql`, `50_fact_flight.sql`, `60_fact_weather.sql`):

| Что | Правило |
|---|---|
| дубли | позиции: одна на (снимок, источник, регион, борт); рейсы: последнее состояние из всех снимков табло; METAR: перекрывающиеся окна запросов схлопываются по (станция, время) |
| типы | время из строк в `DateTime` UTC, время табло из UTC+2 в UTC, видимость `6+` в число, `on_ground` в `UInt8` |
| пропуски | позиции без координат и с некорректным `icao24` отбрасываются |
| выбросы | высота вне −500…20 000 м, скорость вне 0…400 м/с, курс вне 0…360, ветер, температура и давление вне физического диапазона заменяются на NULL, сама запись остаётся |
| задержки | считаются только для вылетевших и севших рейсов, переход через полночь поправлен, задержка больше 4 часов считается ошибкой табло и остаётся NULL |

**Обогащение:** к позиции добавляется версия борта, актуальная на момент снимка
(регистрация, тип, оператор), к рейсу добавляются авиакомпания и аэропорт второго конца,
к погоде аэропорт по ICAO-коду станции.

**Слияние источников и агрегация** (`31_fact_aircraft_snapshot.sql`): один борт в одном
снимке виден разными сетями и двумя регионами. Все эти строки сворачиваются в одну:
координаты берутся у лучшего источника (adsb.lol, затем adsb.fi, затем OpenSky), считается
число сетей, видевших борт, и признак попадания в зону Кейптауна. Это основа для вопроса ЛР3
о покрытии источников.

## 4. Медленно меняющиеся измерения (SCD)

| Измерение | Тип | Почему |
|---|---|---|
| `dim_aircraft` | **SCD2** | регистрация, тип и оператор борта меняются (перекраска, лизинг, смена оператора); анализ за прошлый период должен видеть тогда актуальные значения |
| `dim_aircraft_type`, `dim_airline`, `dim_airport` | SCD1 | названия исправляются, история изменений не нужна |
| `dim_source`, `dim_region`, `dim_date` | статические | справочники |

Как устроен SCD2 в `20_dim_aircraft.sql`:

1. Наблюдения (борт, снимок) берутся только из adsb.lol и adsb.fi: OpenSky не отдаёт
   регистрацию и тип. Если оба источника видят борт, приоритет у непустого значения,
   затем у adsb.lol.
2. Оконной функцией `lagInFrame` ищутся моменты, когда атрибуты отличаются от предыдущего
   наблюдения. Каждый такой момент открывает новую версию.
3. `valid_to` версии равен `valid_from` следующей (`leadInFrame`), у последней он равен
   `2100-01-01`, и `is_current = 1`.
4. Первая версия открыта в прошлое (`valid_from = 1970-01-01`), чтобы факты, снятые
   до первого появления атрибутов, тоже нашли версию.
5. Ключ строки `(icao24, valid_from)` не меняется, поэтому повторный запуск заменяет строки
   (`ReplacingMergeTree` по `row_version`), а не дублирует.
6. Факт находит нужную версию через `ASOF LEFT JOIN` по `snapshot_ts >= valid_from`,
   в `aircraft_sk` ложится ключ версии, действовавшей в момент снимка.

Ограничение: история пересобирается из всего ODS целиком, а не инкрементально. На текущих
объёмах это секунды, при росте ODS на порядки стоит перейти на инкремент.

## 5. Зависимости между ETL-процессами

Детальный слой должен ждать загрузки исходных данных. Использованы два механизма:

1. **Assets Airflow 3** ([dags/datasets.py](dags/datasets.py)). DAGи ODS в конце публикуют
   asset (`ods_positions`, `ods_flights`, `ods_metar`), и DAGи DDS стартуют сами, когда asset
   обновлён. Публикует отдельная задача `publish`, которая зависит от всех трёх задач
   загрузки (по одной на хранилище), поэтому DDS не стартует, пока ClickHouse не дозагрузился.
2. **Сенсор** `ods_ready` (`PythonSensor`, режим `reschedule`) в начале каждого DAGа DDS.
   Он проверяет, что таблица ODS существует и непуста. Это защита для ручного запуска,
   когда asset никто не публиковал.

```
ods_positions ─► [asset] ─► dds_positions ─┐
ods_board     ─► [asset] ─► dds_flights   ─┼─► [assets] ─► dds_quality
ods_weather   ─► [asset] ─► dds_weather   ─┘
ods_archive   ─► все три asset
```

| DAG | Запускается по | Задачи |
|---|---|---|
| `dds_positions` | asset `ods_positions` | ods_ready → schema → dim_aircraft → fact_position → fact_aircraft_snapshot → publish |
| `dds_flights` | asset `ods_flights` | ods_ready → schema → dim_airline_airport → fact_flight → publish |
| `dds_weather` | asset `ods_metar` | ods_ready → schema → fact_weather → publish |
| `dds_quality` | все три DDS-asset | четыре параллельные задачи `check` → verdict |

В DAGи ODS внесено минимальное изменение: задача `publish` и объявление asset. Логика
загрузки не менялась.

## 6. Data Quality

DAG `dds_quality` запускается, когда обновились все три потока DDS. Проверки лежат
в [dags/dq.py](dags/dq.py), каждая это SQL с одним числом и правило «не больше / не меньше
порога». Результат пишется в `dds_dq_results`. Проверка уровня `error` валит DAG,
`warn` только помечается.

| Область | Проверки |
|---|---|
| `positions` | объём ODS и DDS (доля сохранённых не ниже 0.9), слитый факт покрывает все позиции, дубли, координаты, высота, скорость, длина icao24; предупреждения: доля пустых позывных и доля бортов с неизвестными атрибутами |
| `flights` | объём ODS и DDS (не ниже 0.99), дубли, пустой номер рейса, задержка вне ±240 мин; предупреждения: неизвестная авиакомпания и аэропорт |
| `weather` | объём, дубли, ветер, температура, давление; предупреждение: станция не найдена в справочнике |
| `integrity` | ссылки фактов на измерения (источник, регион, дата, борт, авиакомпания, аэропорт), тип борта есть в справочнике типов, SCD2: у каждого борта ровно одна текущая версия и версии идут встык |

Просмотр истории проверок:

```sql
select run_ts, area, check_name, status, value, rule
from dds_dq_results order by run_ts desc, area limit 50
```

## 7. Как воспроизвести

```bash
docker compose up -d
# разовая загрузка собранного вручную в ODS (если data/raw есть)
docker compose exec airflow-scheduler airflow dags trigger ods_archive
```

После `ods_archive` три DAGа DDS стартуют сами по asset, а после них `dds_quality`.
Вручную:

```bash
docker compose exec airflow-scheduler airflow dags trigger dds_positions
docker compose exec airflow-scheduler airflow dags trigger dds_flights
docker compose exec airflow-scheduler airflow dags trigger dds_weather
docker compose exec airflow-scheduler airflow dags trigger dds_quality
```

Проверка результата:

```bash
docker compose exec clickhouse clickhouse-client -u ods --password ods -d ods -q \
  "select 'positions', count() from dds_fact_position final
   union all select 'snapshots', count() from dds_fact_aircraft_snapshot final
   union all select 'flights', count() from dds_fact_flight final
   union all select 'weather', count() from dds_fact_weather final
   union all select 'aircraft versions', count() from dds_dim_aircraft final"
```

Пример SCD2: борты, у которых больше одной версии:

```sql
select icao24, registration, type_code, operator, valid_from, valid_to, is_current
from dds_dim_aircraft final
where icao24 in (select icao24 from dds_dim_aircraft final group by icao24 having count() > 1)
order by icao24, valid_from
```

Пример запроса по факту с измерениями (средняя задержка по авиакомпаниям):

```sql
select a.airline_name, count() as flights, round(avg(f.delay_min), 1) as avg_delay
from (select * from dds_fact_flight final) as f
join (select * from dds_dim_airline final) as a on a.airline_key = f.airline_key
where f.delay_min is not null
group by a.airline_name order by avg_delay desc
```

## 8. Результаты запуска

_Заполняется после первого прогона на реальных данных: скриншоты Airflow (список DAGов,
граф `dds_positions`, зелёный `dds_quality`), счётчики таблиц DDS, содержимое
`dds_dq_results` и пример SCD2._
