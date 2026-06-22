import time
import json
import random
import argparse
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Kafka Configuration
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'sensor_readings_topic'

def main():
    print(f"Initializing Kafka producer connecting to {BOOTSTRAP_SERVERS}...")
    
    # Try connecting to Kafka with retries
    producer = None
    for attempt in range(1, 6):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=5000
            )
            print("Successfully connected to Kafka!")
            break
        except KafkaError as e:
            print(f"Connection attempt {attempt} failed: {e}. Retrying in 3 seconds...")
            time.sleep(3)
            
    if not producer:
        print("Could not connect to Kafka. Make sure your docker containers are running.")
        return

    parser = argparse.ArgumentParser(description="Kafka producer test with duration limit.")
    parser.add_argument("--duration", type=float, default=60.0, help="Duration to run the test in seconds")
    parser.add_argument("--delay", type=float, default=0.001, help="Delay between messages in seconds")
    args = parser.parse_args()

    print(f"Sending messages to Kafka topic '{TOPIC_NAME}' for {args.duration} seconds...")
    
    start_time = time.time()
    stop_time = start_time + args.duration
    sent_count = 0
    
    while time.time() < stop_time:
        # Generate event
        data = {
            'sensor_id': f"sensor_{random.randint(1, 100)}",
            'temperature': round(random.uniform(20.0, 35.0), 2),
            'humidity': round(random.uniform(40.0, 80.0), 2),
            'timestamp': time.time()
        }
        
        # Asynchronous send
        producer.send(TOPIC_NAME, value=data)
        sent_count += 1
        
        # Print progress every 1000 messages
        if sent_count % 1000 == 0:
            print(f"Sent {sent_count} messages...")
            
        if args.delay > 0:
            time.sleep(args.delay)
            
    # Flush makes sure all buffered records are immediately sent
    print("Flushing Kafka producer buffer...")
    producer.flush()
    duration = time.time() - start_time
    
    print("\n--- Kafka Producer Results ---")
    print(f"Total messages sent: {sent_count}")
    print(f"Total duration:      {duration:.2f} seconds")
    print(f"Throughput:          {sent_count / duration:.2f} messages/sec")
    print("\n[OBSERVATION] The producer successfully queued messages into Kafka fast and buffered them in memory/disk.")

if __name__ == '__main__':
    main()
