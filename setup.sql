-- 1. Исходная таблица raw_bets

CREATE TABLE raw_bets (
    user UInt32,
    order UInt32,
    order_position UInt8,
    amount Decimal(15, 2),
    bet_date DateTime,
    ver UInt64 DEFAULT toUnixTimestamp64Milli(now64()) -- Точная версия для дедупликации
) ENGINE = ReplacingMergeTree(ver)
PARTITION BY toYYYYMM(bet_date) -- Партиции по месяцам
ORDER BY (user, order, order_position);

-- 2. Промежуточная таблица unique_orders (уникальные заказы)

CREATE TABLE unique_orders (
    user UInt32,
    order UInt32,
    amount Decimal(15, 2),
    bet_date DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(bet_date)
ORDER BY (user, order);

-- Материализованное представление для дедупликации позиций

CREATE MATERIALIZED VIEW mv_unique_orders
TO unique_orders
AS
SELECT
    user,
    order,
    argMin(amount, order_position) AS amount, -- Берем amount для первой позиции
    any(bet_date) AS bet_date -- Сохраняем дату ставки
FROM raw_bets
GROUP BY user, order;

-- 3. Итоговая таблица user_totals (агрегаты по пользователям)

CREATE TABLE user_totals (
    user UInt32,
    amount Decimal(15, 2)
) ENGINE = SummingMergeTree()
ORDER BY (user);

-- Материализованное представление для агрегации по пользователям

CREATE MATERIALIZED VIEW mv_user_totals
TO user_totals
AS
SELECT
    user,
    sum(amount) AS amount
FROM unique_orders
GROUP BY user;

-- (Опционально) Таблица для проверки исходных данных

CREATE TABLE raw_bets_buffer (
    user UInt32,
    order UInt32,
    order_position UInt8,
    amount Decimal(15, 2),
    bet_date DateTime
) ENGINE = Buffer('default', 'raw_bets', 1, 10, 100, 10000, 1000000);
