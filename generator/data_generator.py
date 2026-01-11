import psycopg2
import time
import random

def main():
    conn = psycopg2.connect(
        host="postgres",
        database="fitness_db",
        user="postgres",
        password="mysecretpassword",
        port="5432"
    )
    
    counter = 0
    try:
        while True:
            user_id = random.randint(1, 3)
            heart_rate = random.randint(60, 120)
            steps = random.randint(0, 100)
            
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO fitness_metrics (user_id, heart_rate, steps) VALUES (%s, %s, %s)",
                    (user_id, heart_rate, steps)
                )
                conn.commit()
            
            counter += 1
            if counter % 10 == 0:
                print(f"Inserted {counter} records")
            
            time.sleep(2)
    except KeyboardInterrupt:
        print(f"\nStopped. Total records: {counter}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()