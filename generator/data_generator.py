import psycopg2
import time
import random
from datetime import datetime
import os
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection(max_retries=30, delay=2):
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "postgres"),
                database=os.getenv("DB_NAME", "fitness_db"),
                user=os.getenv("DB_USER", "postgres"),
                password=os.getenv("DB_PASSWORD", "mysecretpassword"),
                port=os.getenv("DB_PORT", "5432"),
                connect_timeout=5
            )
            logger.info("Подключение к БД установлено")
            return conn
        except Exception as e:
            logger.warning(f"Попытка {i+1}/{max_retries}: БД еще не готова: {e}")
            if i == max_retries - 1:
                logger.error("Не удалось подключиться к PostgreSQL")
                raise
            time.sleep(delay)
    return None

def create_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS fitness_metrics (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        heart_rate INTEGER,
        steps INTEGER,
        calories_burned DECIMAL(5,1),
        activity_type VARCHAR(20),
        recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_recorded_at ON fitness_metrics(recorded_at);
    CREATE INDEX IF NOT EXISTS idx_user_id ON fitness_metrics(user_id);
    CREATE INDEX IF NOT EXISTS idx_activity_type ON fitness_metrics(activity_type);
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
        logger.info("Таблица 'fitness_metrics' готова")
    except Exception as e:
        logger.error(f"Ошибка создания таблицы: {e}")
        conn.rollback()

def generate_fake_fitness_data():
    user_id = random.randint(1, 5)
    activities = ['running', 'walking', 'cycling', 'resting']
    activity = random.choice(activities)
    
    if activity == 'running':
        heart_rate = random.randint(140, 180)
        steps = random.randint(150, 300)
        calories = round(random.uniform(10.0, 15.0), 1)
    elif activity == 'walking':
        heart_rate = random.randint(90, 130)
        steps = random.randint(80, 150)
        calories = round(random.uniform(5.0, 8.0), 1)
    elif activity == 'cycling':
        heart_rate = random.randint(120, 160)
        steps = 0
        calories = round(random.uniform(8.0, 12.0), 1)
    else: 
        heart_rate = random.randint(60, 90)
        steps = random.randint(0, 10)
        calories = round(random.uniform(0.5, 2.0), 1)
    
    return (user_id, heart_rate, steps, calories, activity)

def main():    
    conn = get_db_connection()
    create_table(conn)
    
    counter = 0
    try:
        while True:
            try:
                if conn.closed:
                    logger.warning("Соединение разорвано, переподключаемся...")
                    conn = get_db_connection(max_retries=5, delay=1)
                
                data = generate_fake_fitness_data()
                with conn.cursor() as cur:
                    sql = """
                    INSERT INTO fitness_metrics 
                    (user_id, heart_rate, steps, calories_burned, activity_type)
                    VALUES (%s, %s, %s, %s, %s)
                    """
                    cur.execute(sql, data)
                    conn.commit()
                
                counter += 1
                
                if counter % 10 == 0:
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    logger.info(f"[{timestamp}] Записей: {counter}, "
                          f"Пользователь {data[0]}, {data[4]}, пульс: {data[1]}")
                
                time.sleep(1)
                
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                logger.error(f"Ошибка БД: {e}")
                time.sleep(2)
                try:
                    if conn:
                        conn.close()
                    conn = get_db_connection(max_retries=5, delay=1)
                except:
                    pass
            
    except KeyboardInterrupt:
        logger.info(f"\n\nОстановлено. Всего записей: {counter}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
    finally:
        if conn and not conn.closed:
            conn.close()
            logger.info("Подключение к БД закрыто")

if __name__ == "__main__":
    main()