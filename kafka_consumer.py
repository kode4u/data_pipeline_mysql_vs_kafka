import time
import json
import mysql.connector
from mysql.connector import Error
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Kafka Config
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'sensor_readings_topic'

# MySQL Config
DB_CONFIG = {
    'host': 'localhost',
    'user': 'pipeline_user',
    'password': 'pipeline_password',
    'database': 'pipeline_db',
    'port': 3307,
    'use_pure': True
}

BATCH_SIZE = 50
FLUSH_INTERVAL_SEC = 1.0

def get_db_connection():
    while True:
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            return conn
        except Error as e:
            print(f"Database connection failed: {e}. Retrying in 2 seconds...")
            time.sleep(2)

def main():
    print(f"Connecting to Kafka on {BOOTSTRAP_SERVERS}...")
    consumer = None
    for attempt in range(1, 10):
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='sensor-pipeline-group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print("Successfully connected to Kafka!")
            break
        except KafkaError as e:
            print(f"Kafka connection attempt {attempt} failed: {e}. Retrying in 3 seconds...")
            time.sleep(3)
            
    if not consumer:
        print("Could not connect to Kafka. Exiting.")
        return

    print("Connecting to MySQL Database...")
    db_conn = get_db_connection()
    cursor = db_conn.cursor()
    print("Successfully connected to MySQL database!")

    print(f"\nListening for messages on topic '{TOPIC_NAME}'...")
    print("Controlled execution: Batching inserts up to 50 rows or every 1.0 second.")
    
    batch = []
    last_flush_time = time.time()
    total_processed = 0

    try:
        while True:
            # Poll Kafka for messages (non-blocking after timeout)
            msg_pack = consumer.poll(timeout_ms=500)
            
            for tp, messages in msg_pack.items():
                for message in messages:
                    data = message.value
                    batch.append((data['sensor_id'], data['temperature'], data['humidity']))
            
            current_time = time.time()
            # If batch is full, or flush interval has elapsed
            if len(batch) >= BATCH_SIZE or (current_time - last_flush_time >= FLUSH_INTERVAL_SEC and len(batch) > 0):
                try:
                    # Batch insert
                    query = "INSERT INTO sensor_readings (sensor_id, temperature, humidity) VALUES (%s, %s, %s)"
                    cursor.executemany(query, batch)
                    db_conn.commit()
                    
                    total_processed += len(batch)
                    print(f"Successfully flushed {len(batch)} readings to MySQL. Total inserted: {total_processed}")
                    
                except Error as db_err:
                    print(f"Error inserting batch: {db_err}. Rolling back.")
                    db_conn.rollback()
                    # Reconnect if connection was lost
                    if not db_conn.is_connected():
                        print("Reconnecting to database...")
                        db_conn = get_db_connection()
                        cursor = db_conn.cursor()
                finally:
                    batch = []
                    last_flush_time = current_time

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        # Flush any remaining items
        if batch:
            try:
                query = "INSERT INTO sensor_readings (sensor_id, temperature, humidity) VALUES (%s, %s, %s)"
                cursor.executemany(query, batch)
                db_conn.commit()
                total_processed += len(batch)
                print(f"Flushed final {len(batch)} readings. Total inserted: {total_processed}")
            except Error as e:
                print(f"Failed to flush final batch: {e}")
        
        if db_conn and db_conn.is_connected():
            cursor.close()
            db_conn.close()
        if consumer:
            consumer.close()
        print("Consumer shut down cleanly.")

if __name__ == '__main__':
    main()
