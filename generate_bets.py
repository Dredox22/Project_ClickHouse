import csv
import random
from faker import Faker
import time

# Инициализация Faker
fake = Faker()

# Параметры
NUM_ROWS = 1_000_000  # Общее количество строк
NUM_USERS = 10_000    # Количество уникальных пользователей
MAX_ORDERS_PER_USER = 50  # Максимум ставок на пользователя
MAX_POSITIONS_PER_ORDER = 5  # Максимум позиций в ставке

# Функция для генерации данных
def generate_bets(num_rows, num_users, max_orders_per_user, max_positions_per_order):
    data = []
    order_counter = 1  # Глобальный счетчик заказов
    
    rows_generated = 0
    while rows_generated < num_rows:
        user_id = random.randint(1, num_users)
        num_orders = random.randint(1, min(max_orders_per_user, (num_rows - rows_generated) // max_positions_per_order))
        
        for _ in range(num_orders):
            if rows_generated >= num_rows:
                break
            num_positions = random.randint(1, max_positions_per_order)
            amount = round(random.uniform(10.00, 500.00), 2)
            bet_date = fake.date_time_this_decade().strftime("%Y-%m-%d %H:%M:%S")
            
            for pos in range(1, num_positions + 1):
                if rows_generated >= num_rows:
                    break
                data.append({
                    "user": user_id,
                    "order": order_counter,
                    "order_position": pos,
                    "amount": amount,
                    "bet_date": bet_date
                })
                rows_generated += 1
            order_counter += 1
    
    return data

# Генерация данных
print("Генерация 1 млн строк...")
start_time = time.time()
bets_data = generate_bets(NUM_ROWS, NUM_USERS, MAX_ORDERS_PER_USER, MAX_POSITIONS_PER_ORDER)
end_time = time.time()
print(f"Генерация завершена за {end_time - start_time:.2f} секунд")

# Запись в CSV
with open("raw_bets_1m.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["user", "order", "order_position", "amount", "bet_date"])
    writer.writeheader()
    writer.writerows(bets_data)

print(f"Создан файл raw_bets_1m.csv с {len(bets_data)} строками")