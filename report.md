# Отчёт по практической №1

## 1. Вариант и масштаб

| Поле | Значение |
|---|---|
| ФИО (как в VK Education) | Лукьяненко Всеволод |
| Вариант (запрос) | Q18 — Large Volume Customer |
| Масштаб sfN | sf1 |

**Почему выбран масштаб:** изначально работа выполнялась на sf10, но при построении Silver-слоя запрос Q18 превысил per-node memory limit Trino 1.79 GB. Поэтому для итоговой практической был выбран sf1: он сохраняет полную логику Q18 и позволяет стабильно выполнить все этапы Bronze → Silver → Gold на локальном одноузловом стенде. Один и тот же масштаб sf1 использован на всех слоях.

## 2. Поднятые сервисы — и почему именно они

| Сервис | Роль в стеке | Почему именно он |
|---|---|---|
| MinIO | S3-совместимое объектное хранилище для файлов Iceberg | Даёт локальное S3-совместимое хранилище и позволяет воспроизвести архитектуру объектного storage без облака. |
| PostgreSQL | БД для внутренних метаданных Lakekeeper | Надёжная реляционная БД, поддерживаемая Lakekeeper; удобна для локального запуска в Docker. |
| Lakekeeper | Iceberg REST Catalog | Хранит и отдаёт метаданные Iceberg-таблиц через REST API, отделяя каталог от вычислительного движка. |
| Trino | Основной SQL-движок | Поддерживает TPC-H, Iceberg и REST Catalog; подходит для интерактивных SQL-запросов и построения слоёв Lakehouse. |
| Apache Spark | Дополнительный движок | Подключается к тому же Iceberg-каталогу и читает те же данные, что и Trino; использован для проверки совместимости Lakehouse с другим движком. |

### Скрипты, которыми поднимался стек

```bash
# Docker-сеть для связи контейнеров
docker network create lakehouse-net

# MinIO
docker run -d --name lakehouse-minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -v minio-data:/data \
  bitnamilegacy/minio:latest \
  server /data --console-address ":9001"

# PostgreSQL
docker run -d --name lakehouse-pg \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -v pg-data:/var/lib/postgresql/data \
  postgres:17

docker network connect lakehouse-net lakehouse-pg
docker network connect lakehouse-net lakehouse-minio

# Миграции Lakekeeper
docker run --rm \
  --network lakehouse-net \
  -e 'LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!' \
  -e 'LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakehouse-pg:5432/postgres' \
  -e 'LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakehouse-pg:5432/postgres' \
  quay.io/lakekeeper/catalog:latest-main migrate

# Lakekeeper
docker run -d --name lakekeeper \
  --network lakehouse-net \
  -e 'LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!' \
  -e 'LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakehouse-pg:5432/postgres' \
  -e 'LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakehouse-pg:5432/postgres' \
  -p 8181:8181 \
  quay.io/lakekeeper/catalog:latest-main serve

# Trino
MSYS_NO_PATHCONV=1 docker run -d --name trino \
  --network lakehouse-net \
  -p 8080:8080 \
  -v "$(pwd -W)/catalog:/etc/trino/catalog" \
  trinodb/trino:476
```

Проверка работы сервисов:

```bash
docker ps
curl http://localhost:8181/health
curl http://localhost:8080/v1/info
docker exec trino trino --execute "SHOW CATALOGS"
```

В Trino были доступны каталоги `bench`, `lakekeeper`, `system`, `tpch`.

## 3. Разбор запроса — какие данные нужны

Мой вариант — TPC-H Q18 Large Volume Customer. Запрос ищет крупные заказы, в которых суммарное количество товара больше 300, связывает заказ с клиентом и выводит самые дорогие заказы.

| Таблица TPC-H | Нужные колонки | Что запрос с ними делает |
|---|---|---|
| customer | custkey, name | custkey используется в JOIN с orders, name выводится в результате. |
| orders | orderkey, custkey, orderdate, totalprice | JOIN с customer и lineitem; orderdate и totalprice выводятся и используются при сортировке. |
| lineitem | orderkey, quantity | По orderkey выполняется JOIN; quantity агрегируется, крупные заказы выбираются условием HAVING sum(quantity) > 300. |

Исходная логика Q18:

```sql
SELECT
    c.name,
    c.custkey,
    o.orderkey,
    o.orderdate,
    o.totalprice,
    sum(l.quantity) AS total_quantity
FROM tpch.sf1.customer AS c
JOIN tpch.sf1.orders AS o
    ON c.custkey = o.custkey
JOIN tpch.sf1.lineitem AS l
    ON o.orderkey = l.orderkey
WHERE o.orderkey IN (
    SELECT orderkey
    FROM tpch.sf1.lineitem
    GROUP BY orderkey
    HAVING sum(quantity) > 300
)
GROUP BY
    c.name,
    c.custkey,
    o.orderkey,
    o.orderdate,
    o.totalprice
ORDER BY o.totalprice DESC, o.orderdate
LIMIT 100;
```

## 4. Медальон — решения по каждому слою

### Bronze

В Bronze были материализованы только нужные для Q18 колонки. Формат — ORC, основной кодек — ZSTD, партиционирование не использовалось.

| Таблица | Колонки | Строк | Вес, МБ | Формат | Сжатие | Партиционирование | Обоснование |
|---|---|---:|---:|---|---|---|---|
| customer | custkey, name | 150 000 | 0.1 | ORC | ZSTD | нет | Оставлены только колонки, необходимые для JOIN и вывода. На sf1 таблица небольшая, поэтому отдельное партиционирование не требуется. |
| orders | orderkey, custkey, orderdate, totalprice | 1 500 000 | 12.0 | ORC | ZSTD | нет | Содержит ключи для JOIN и поля итогового запроса; ZSTD выбран как основной компромисс между размером и скоростью. |
| lineitem | orderkey, quantity | 6 001 215 | 12.6 | ORC | ZSTD | нет | Это самая крупная таблица Q18; оставлены только ключ заказа и количество, необходимые для агрегации. |

Создание Bronze:

```sql
SET SESSION bench.compression_codec = 'ZSTD';

CREATE TABLE bench.bronze.customer
WITH (format = 'ORC')
AS SELECT custkey, name
FROM tpch.sf1.customer;

CREATE TABLE bench.bronze.orders
WITH (format = 'ORC')
AS SELECT orderkey, custkey, orderdate, totalprice
FROM tpch.sf1.orders;

CREATE TABLE bench.bronze.lineitem
WITH (format = 'ORC')
AS SELECT orderkey, quantity
FROM tpch.sf1.lineitem;
```

### Silver

В Silver был выполнен JOIN трёх Bronze-таблиц и агрегирование по заказу. Оставлены только заказы с `sum(quantity) > 300`.

| Таблица | Колонки | Строк | Вес | Формат | Сжатие | Партиционирование | Обоснование |
|---|---|---:|---|---|---|---|---|
| q18_large_orders | name, custkey, orderkey, orderdate, totalprice, total_quantity | 57 | 0.0021 МБ (2.1 КБ) | ORC | ZSTD | нет | Silver уже содержит очищенный и агрегированный набор крупных заказов. Таблица очень маленькая, поэтому партиционирование не нужно. |

SQL:

```sql
SET SESSION bench.compression_codec = 'ZSTD';

CREATE TABLE bench.silver.q18_large_orders
WITH (format = 'ORC')
AS
SELECT
    c.name,
    c.custkey,
    o.orderkey,
    o.orderdate,
    o.totalprice,
    sum(l.quantity) AS total_quantity
FROM bench.bronze.customer c
JOIN bench.bronze.orders o
    ON c.custkey = o.custkey
JOIN bench.bronze.lineitem l
    ON o.orderkey = l.orderkey
GROUP BY
    c.name,
    c.custkey,
    o.orderkey,
    o.orderdate,
    o.totalprice
HAVING sum(l.quantity) > 300;
```

### Gold

Gold — готовая витрина для ответа на Q18: топ заказов по стоимости.

| Таблица | Колонки | Строк | Вес | Формат | Сжатие | Партиционирование | Обоснование |
|---|---|---:|---|---|---|---|---|
| q18_top100 | name, custkey, orderkey, orderdate, totalprice, total_quantity | 57 | 0.0021 МБ (2.1 КБ) | ORC | ZSTD | нет | Это итоговая витрина. На sf1 условию Q18 соответствуют только 57 заказов, поэтому LIMIT 100 оставляет все 57 строк. |

SQL:

```sql
SET SESSION bench.compression_codec = 'ZSTD';

CREATE TABLE bench.gold.q18_top100
WITH (format = 'ORC')
AS
SELECT
    name,
    custkey,
    orderkey,
    orderdate,
    totalprice,
    total_quantity
FROM bench.silver.q18_large_orders
ORDER BY totalprice DESC, orderdate
LIMIT 100;
```

Пример первых строк Gold:

| Клиент | custkey | orderkey | orderdate | totalprice | total_quantity |
|---|---:|---:|---|---:|---:|
| Customer#000128120 | 128120 | 4722021 | 1994-04-07 | 544089.09 | 323 |
| Customer#000144617 | 144617 | 3043270 | 1997-02-12 | 530604.44 | 317 |
| Customer#000013940 | 13940 | 2232932 | 1997-04-13 | 522720.61 | 304 |
| Customer#000066790 | 66790 | 2199712 | 1996-09-30 | 515531.82 | 327 |

## 5. Тесты производительности

Для сравнения были созданы два одинаковых Bronze-набора на sf1 и ORC, отличающиеся только кодеком сжатия: ZSTD и GZIP. После этого на них запускался один и тот же Q18.

| Сжатие на Bronze | Время запроса | Размер Bronze, МБ | Вывод |
|---|---:|---:|---|
| GZIP | 3.675 с | 24.6 | Немного компактнее, но запрос выполнялся медленнее. |
| ZSTD | 3.255 с | 24.7 | Практически тот же размер, но Q18 выполнился быстрее. |

В моём эксперименте GZIP сжал Bronze лишь на 0.1 МБ лучше. Разница составляет около 0.4%, то есть практически несущественна. При этом Q18 на ZSTD выполнился примерно на 11.4% быстрее. Поэтому для данного набора данных ZSTD оказался более выгодным компромиссом между размером и скоростью.

## 6. Часть 2 — осмысление и работа с инструментами

### 6.1. Понять и объяснить

**Зачем нужны большие данные и lakehouse, а не обычная БД/хранилище?**

Lakehouse позволяет хранить большие объёмы файлов в дешёвом объектном хранилище и при этом работать с ними как с таблицами через SQL. В моём стенде данные физически лежат в MinIO, Iceberg задаёт структуру таблиц и снапшоты, а Trino и Spark могут читать одни и те же данные. Это удобнее, когда объёмы растут и нужно отделить хранение от вычислений.

**Пример schema evolution на Silver: добавил/переименовал колонку — что произошло?**

В `bench.silver.q18_large_orders` была добавлена колонка `load_source varchar`, после чего она была переименована в `source_system`. Таблицу и существующие строки не пришлось пересоздавать. После DESCRIBE новая схема сразу стала видна. Старые данные сохранились, а новая колонка для старых строк имеет NULL.

**Пример восстановления/чтения данных из снапшота (time travel)**

До вставки тестовой строки snapshot имел id `3506661052073598753`. После вставки TEST_CUSTOMER появился новый snapshot `3505617063857125852`. В текущей версии тестовая строка видна, а запрос `FOR VERSION AS OF 3506661052073598753` её не возвращает. Это показывает возможность чтения предыдущего состояния таблицы.

#### Schema evolution

```sql
ALTER TABLE bench.silver.q18_large_orders
ADD COLUMN load_source varchar;

ALTER TABLE bench.silver.q18_large_orders
RENAME COLUMN load_source TO source_system;

DESCRIBE bench.silver.q18_large_orders;
```

Результат после переименования:

```text
name            varchar
custkey         bigint
orderkey        bigint
orderdate       date
totalprice      double
total_quantity  double
source_system   varchar
```

#### Time travel

Список снапшотов:

```sql
SELECT committed_at, snapshot_id, operation
FROM bench.silver."q18_large_orders$snapshots"
ORDER BY committed_at;
```

Полученные snapshots:

```text
2026-10-07 08:06:16.012 UTC  3506661052073598753  append
2026-10-07 08:19:03.354 UTC  3505617063857125852  append
```

Для создания новой версии была вставлена тестовая строка:

```sql
INSERT INTO bench.silver.q18_large_orders
(name, custkey, orderkey, orderdate, totalprice, total_quantity, source_system)
VALUES
('TEST_CUSTOMER', 999999999, 999999999,
 DATE '2026-10-07', 999999.99, 999.0, 'manual_test');
```

В текущем состоянии:

```sql
SELECT name, custkey, orderkey
FROM bench.silver.q18_large_orders
WHERE name = 'TEST_CUSTOMER';
```

Результат:

```text
TEST_CUSTOMER | 999999999 | 999999999
```

Чтение старого snapshot:

```sql
SELECT name, custkey, orderkey
FROM bench.silver.q18_large_orders
FOR VERSION AS OF 3506661052073598753
WHERE name = 'TEST_CUSTOMER';
```

Результат пустой, потому что на момент первого snapshot тестовая строка ещё не существовала.

Дополнительно при попытке выбрать `source_system` из старого snapshot Trino сообщил, что такой колонки нет. Это показывает, что time travel затрагивает не только данные, но и состояние схемы таблицы.

### 6.2. Подключить инструмент к lakehouse

**Инструмент: Apache Spark.** Spark умеет работать с Iceberg и подключаться к тому же REST Catalog Lakekeeper. Это позволило проверить, что данные Bronze не привязаны только к Trino и могут читаться другим вычислительным движком без копирования.

Spark запускался в Docker и подключался к той же сети:

```bash
MSYS_NO_PATHCONV=1 docker run -it --rm \
  --network lakehouse-net \
  -v "$(pwd -W)/spark/conf:/opt/spark/conf" \
  -v "$(pwd -W)/spark/jars:/opt/spark/iceberg-jars" \
  apache/spark:3.5.3 \
  /opt/spark/bin/spark-sql
```

Проверка таблиц Bronze:

```sql
SHOW TABLES IN bench.bronze;
```

Результат:

```text
customer
orders
lineitem
```

Чтение данных:

```sql
SELECT *
FROM bench.bronze.customer
LIMIT 10;
```

Spark успешно вернул строки customer.

Проверка количества строк:

```sql
SELECT count(*)
FROM bench.bronze.lineitem;
```

Результат: `6001215`.

Таким образом, Spark прочитал ту же Iceberg-таблицу Bronze, которая ранее создавалась через Trino.

## 7. Выводы

В ходе работы была построена медальонная архитектура Bronze → Silver → Gold для запроса TPC-H Q18. Bronze хранит минимально необходимые исходные данные, Silver выполняет объединение и выделяет крупные заказы, а Gold представляет готовую аналитическую витрину.

При сравнении кодеков GZIP и ZSTD на моём стенде ZSTD оказался предпочтительнее: размер практически не изменился, но запрос выполнялся быстрее. Iceberg также позволил без переписывания данных изменить схему таблицы и прочитать её старую версию через snapshot. Подключение Spark показало, что один и тот же Lakehouse может использоваться несколькими вычислительными движками без копирования данных.
