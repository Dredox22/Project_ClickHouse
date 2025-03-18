# Betting Data Aggregation in ClickHouse

Этот проект реализует инкрементальную агрегацию данных ставок из потоковой таблицы `raw_bets` в ClickHouse, исключая дубли и зависимости от `OPTIMIZE`.

## Описание задачи

- **Входные данные**: Таблица `raw_bets` с полями `user`, `order`, `order_position`, `amount`, `bet_date`.
  - У одного пользователя много ставок.
  - У каждой ставки несколько позиций.
  - `amount` одинаков для всех позиций внутри одной ставки.
- **Цель**: Таблица `user_totals` с агрегатами:

  ```
  user | amount
  1    | 150.00
  2    | 75.00
  ```

  где `amount` — сумма уникальных ставок, а не позиций.
- **Условия**:
  - Данные потоковые, объем — миллионы строк.
  - Подсчеты инкрементальные.
  - Без `OPTIMIZE` и `FINAL`.

## Структура проекта

- `generate_bets.py`: Генерирует 1 млн строк в `raw_bets_1m.csv`.
- `setup.sql`: Создает таблицы и MV в ClickHouse.
- `raw_bets_1m.csv`: Сгенерированный файл данных.
- `README.md`: Документация.

## Требования

- **Python 3.8+**: `pip install faker`
- **ClickHouse**: Версия 23.8+.
- **Диск**: ~200 МБ для 1 млн строк.

## Установка и запуск

### 1. Генерация данных

1. Сохраните `generate_bets.py`.
2. Установите зависимости:
   ```bash
   pip install faker
   ```
3. Запустите:
   ```bash
   python generate_bets.py
   ```

### 2. Настройка ClickHouse

Запустите контейнер (если не запущен):
```bash
docker run -d --name clickhouse-server -p 9000:9000 clickhouse/clickhouse-server
```
Скопируйте и выполните `setup.sql`:
```bash
docker cp setup.sql clickhouse-server:/tmp/setup.sql
docker exec -i clickhouse-server clickhouse-client < setup.sql
```

### 3. Импорт данных

Скопируйте и загрузите данные:
```bash
docker cp raw_bets_1m.csv clickhouse-server:/tmp/raw_bets_1m.csv
docker exec -i clickhouse-server clickhouse-client --query="INSERT INTO raw_bets_buffer FORMAT CSVWithNames" < raw_bets_1m.csv
```

### 4. Проверка

```bash
docker exec -it clickhouse-server clickhouse-client --query="SELECT * FROM user_totals ORDER BY user LIMIT 5"
```

## Архитектура

- **raw_bets_buffer**: Буфер для потоковых вставок.
- **raw_bets**: Хранит сырые данные (MergeTree).
- **mv_unique_orders**: Дедуплицирует позиции через `argMin`, записывает в `unique_orders`.
- **mv_user_totals**: Агрегирует суммы в `user_totals` (ReplacingMergeTree).

## Пример

### Вход (raw_bets):

```
user,order,order_position,amount,bet_date
1,1,1,100.00,2023-05-12 14:23:11
1,1,2,100.00,2023-05-12 14:23:11
1,2,1,50.00,2021-08-19 09:15:32
```

### Выход (user_totals):

```
user | amount
1    | 150.00
```

## Оптимизация

- **Партиционирование**: `toYYYYMM(bet_date)`.
- **Дедупликация**: `argMin` в MV.
- **Поток**: Buffer для вставок.

## Замечания

- Разовая инициализация в `setup.sql` учитывает существующие данные.
- `ReplacingMergeTree` в `user_totals` хранит только актуальные суммы.

---

### Как запустить

1. **Генерация данных:**
   ```bash
   python generate_bets.py
   ```

2. **Запуск ClickHouse (если не запущен):**
   ```bash
   docker run -d --name clickhouse-server -p 9000:9000 clickhouse/clickhouse-server
   ```

3. **Копирование и выполнение:**
   ```bash
   docker cp setup.sql clickhouse-server:/tmp/setup.sql
   docker exec -i clickhouse-server clickhouse-client < setup.sql
   docker cp raw_bets_1m.csv clickhouse-server:/tmp/raw_bets_1m.csv
   docker exec -i clickhouse-server clickhouse-client --query="INSERT INTO raw_bets_buffer FORMAT CSVWithNames" < raw_bets_1m.csv
   ```

4. **Проверка:**
   ```bash
   docker exec -it clickhouse-server clickhouse-client --query="SELECT * FROM user_totals LIMIT 5"
   ```

## Финальные улучшения

- **Buffer**: Обеспечивает плавную потоковую вставку.
- **MergeTree вместо ReplacingMergeTree**: Убирает зависимость от асинхронного слияния.
- **Инициализация**: Разовая вставка в конце `setup.sql` гарантирует учет всех данных.
- **ReplacingMergeTree в user_totals**: Хранит только актуальные агрегации.