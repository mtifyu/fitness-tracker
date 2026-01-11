import psycopg2
import time
import random
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_table_if_not_exists(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS fitness_metrics (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        heart_rate INTEGER,
        steps INTEGER,
        recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
        logger.info("Table 'fitness_metrics' created or already exists")
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        conn.rollback()

def main():
    logger.info("Starting improved fitness data generator...")
    
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="fitness_db",
            user="postgres",
            password="mysecretpassword",
            port="5432"
        )
        create_table_if_not_exists(conn)
        
        counter = 0
        while True:
            user_id = random.randint(1, 5)
            heart_rate = random.randint(60, 180)
            steps = random.randint(0, 200)
            
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO fitness_metrics (user_id, heart_rate, steps) VALUES (%s, %s, %s)",
                    (user_id, heart_rate, steps)
                )
                conn.commit()
            
            counter += 1
            if counter % 5 == 0:
                timestamp = datetime.now().strftime("%H:%M:%S")
                logger.info(f"[{timestamp}] Records: {counter}, User: {user_id}, HR: {heart_rate}")
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info(f"Stopped. Total records: {counter}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()