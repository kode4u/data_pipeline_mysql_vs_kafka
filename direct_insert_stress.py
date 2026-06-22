import time
import random
import threading
import argparse
import mysql.connector
from mysql.connector import Error

# Database connection details
DB_CONFIG = {
    'host': 'localhost',
    'user': 'pipeline_user',
    'password': 'pipeline_password',
    'database': 'pipeline_db',
    'port': 3307,
    'use_pure': True
}

# Number of concurrent threads to simulate a flood
# MySQL default max_connections is 151. Setting NUM_THREADS to 200 guarantees hitting it.
NUM_THREADS = 200 
INSERTS_PER_THREAD = 5

# Stats counters
success_count = 0
failure_count = 0
stats_lock = threading.Lock()

def stress_worker(worker_id, stop_time):
    global success_count, failure_count
    while time.time() < stop_time:
        conn = None
        try:
            # Attempt to connect to MySQL directly (no pooling)
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            
            # Simulate sensor event details
            sensor_id = f"sensor_{random.randint(1, 100)}"
            temp = round(random.uniform(20.0, 35.0), 2)
            hum = round(random.uniform(40.0, 80.0), 2)
            
            cursor.execute(
                "INSERT INTO sensor_readings (sensor_id, temperature, humidity) VALUES (%s, %s, %s)",
                (sensor_id, temp, hum)
            )
            conn.commit()
            
            with stats_lock:
                success_count += 1
                
        except Error as e:
            # MySQL connection limits or overload will trigger exceptions here
            with stats_lock:
                failure_count += 1
            print(f"[Worker-{worker_id}] Error: {e}")
            
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()
        time.sleep(0.01)

def main():
    parser = argparse.ArgumentParser(description="Direct MySQL stress test with duration limit.")
    parser.add_argument("--duration", type=float, default=60.0, help="Duration to run the test in seconds")
    parser.add_argument("--threads", type=int, default=NUM_THREADS, help="Number of concurrent threads")
    args = parser.parse_args()

    print(f"Starting direct MySQL stress test for {args.duration} seconds...")
    print(f"Simulating {args.threads} concurrent threads...")
    
    start_time = time.time()
    stop_time = start_time + args.duration
    threads = []
    
    for i in range(args.threads):
        t = threading.Thread(target=stress_worker, args=(i, stop_time))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    duration = time.time() - start_time
    
    print("\n--- Direct MySQL Stress Test Results ---")
    print(f"Successful inserts:     {success_count}")
    print(f"Failed inserts:         {failure_count}")
    print(f"Total duration:         {duration:.2f} seconds")
    print(f"Throughput:             {success_count / duration:.2f} successful inserts/sec")
    
    if failure_count > 0:
        print("\n[OBSERVATION] The database suffered from direct flooding! Under high concurrency, connection limits (max_connections) or write capacity issues caused queries to fail.")
    else:
        print("\n[OBSERVATION] All inserts succeeded. Try increasing --threads or running on a lower-spec setup to trigger connection limit errors.")

if __name__ == '__main__':
    main()
