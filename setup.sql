-- 1. Буферная таблица для потоковых вставок
CREATE TABLE raw_bets_buffer (
    user UInt32,
    order UInt32,
    order_position UInt8,
    amount Decimal(15, 2),
    bet_date DateTime
) ENGINE = Buffer('default', 'raw_bets', 1, 10, 100, 10000, 1000000);

-- 2. Основная таблица для сырых данных
CREATE TABLE raw_bets (
    user UInt32,
    order UInt32,
    order_position UInt8,
    amount Decimal(15, 2),
    bet_date DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(bet_date)
ORDER BY (user, order, order_position);

-- 3. Промежуточная таблица для уникальных заказов
CREATE TABLE unique_orders (
    user UInt32,
    order UInt32,
    amount Decimal(15, 2),
    bet_date DateTime,
    insert_time DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(bet_date)
ORDER BY (user, order, insert_time);

-- Материализованное представление для дедупликации позиций
CREATE MATERIALIZED VIEW mv_unique_orders
TO unique_orders
AS
SELECT
    user,
    order,
    argMin(amount, order_position) AS amount,
    any(bet_date) AS bet_date,
    now() AS insert_time
FROM raw_bets
GROUP BY user, order;

-- 4. Итоговая таблица для агрегатов по пользователям
CREATE TABLE user_totals (
    user UInt32,
    amount Decimal(15, 2),
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (user);

-- Материализованное представление для агрегации
CREATE MATERIALIZED VIEW mv_user_totals
TO user_totals
AS
SELECT
    user,
    sum(amount) AS amount,
    toUnixTimestamp(now()) AS version
FROM unique_orders
GROUP BY user
SETTINGS materialize_skip_existing_data = 0;

-- Инициализация существующих данных (выполняется разово после создания таблиц)
INSERT INTO unique_orders
SELECT
    user,
    order,
    argMin(amount, order_position) AS amount,
    any(bet_date) AS bet_date,
    now() AS insert_time
FROM raw_bets
GROUP BY user, order;

INSERT INTO user_totals
SELECT
    user,
    sum(amount) AS amount,
    toUnixTimestamp(now()) AS version
FROM unique_orders
GROUP BY user;
