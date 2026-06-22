# MySQL vs. Kafka Data Pipeline Load Testing

This repository provides a hands-on demonstration and load testing lab comparing **Direct Database Ingestion** with **Queue-based Ingestion (Kafka + Consumer)**. It is designed for students to understand system bottlenecks, connection limits, and how message queues add decoupling and resilience to data pipelines.

---

## Architecture Overview

```mermaid
graph TD
    subgraph "Direct Ingestion (High Contention)"
        A["direct_insert_stress.py (200 Threads)"] -->|Concurrent Inserts| B[("MySQL Database (port 3307)")]
    end

    subgraph "Kafka Pipeline (Decoupled & Resilient)"
        C["kafka_producer.py"] -->|Asynchronous Send| D["Kafka Topic (port 9092)"]
        D -->|Poll & Batch (50 rows/sec)| E["kafka_consumer.py"]
        E -->|Controlled Batch Write| F[("MySQL Database (port 3307)")]
    end
    
    style B fill:#ffebee,stroke:#c62828,stroke-width:2px
    style F fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
```

1. **Direct Ingestion Test**: Simulates concurrent clients flooding the database directly with writes. Demonstrates MySQL `Too many connections` limits and capacity issues under spike loads.
2. **Kafka Pipeline Test**: Simulates clients writing rapidly to Kafka. A consumer reads messages in batches and writes them at a controlled pace to the database. Demonstrates buffering and steady write throughput.

---

## Getting Started

### 1. Prerequisites
Make sure you have the following installed on your machine:
*   [Docker Desktop](https://www.docker.com/products/docker-desktop/)
*   [Python 3.10+](https://www.python.org/downloads/)

### 2. Start the Infrastructure
Spin up the MySQL database and Kafka broker containers in the background:
```bash
docker compose up -d
```
*Verify containers are running:*
```bash
docker compose ps
```

### 3. Setup Python Virtual Environment
Initialize a virtual environment and install the required dependencies:
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# .\venv\Scripts\activate

# Install requirements
pip install -r requirements.txt
```

---

## Running the Stress Tests

### Test 1: Direct Database Ingestion (Direct Flooding)
Run this command to simulate 200 concurrent threads making inserts directly into the MySQL database for **1 minute**:
```bash
python direct_insert_stress.py --duration 60
```

#### What to observe:
*   You will see multiple errors printed: `Error: 1040: Too many connections`.
*   Under high concurrency, the database exceeds its connection limits, resulting in failed writes and lost data.

---

### Test 2: Kafka Queue Ingestion (Resilient Pipeline)
In this test, data is sent to Kafka first, and then written to the database in a controlled batch process.

#### Step 2a: Start the Consumer
Open a terminal, activate your virtual environment, and run the consumer. It will stay open to listen and batch-write incoming records:
```bash
python kafka_consumer.py
```

#### Step 2b: Run the Producer (in another terminal)
Open a new terminal window, activate your virtual environment, and run the producer for **1 minute**:
```bash
python kafka_producer.py --duration 60
```

#### What to observe:
*   The producer sends and queues messages into Kafka extremely fast without any errors.
*   The consumer reads from Kafka, batching them into groups of 50 or flushing every 1.0 second, maintaining database stability.
*   Zero writes are failed or lost.

---

## Checking the Database Records
You can run these commands in your terminal to inspect the database:

*   **Check the total count of inserted records:**
    ```bash
    docker exec -it pipeline-mysql mysql -u pipeline_user -ppipeline_password pipeline_db -e "SELECT COUNT(*) FROM sensor_readings;"
    ```
*   **See the latest 10 records inserted:**
    ```bash
    docker exec -it pipeline-mysql mysql -u pipeline_user -ppipeline_password pipeline_db -e "SELECT * FROM sensor_readings ORDER BY id DESC LIMIT 10;"
    ```

---

## Educational Takeaways
1.  **Connection Saturation**: Databases have limits on concurrent connections (`max_connections`). Bypassing this threshold causes client failures.
2.  **Backpressure & Queuing**: Kafka acts as a buffer. Under load spikes, the producer sends data instantly to Kafka, while the consumer processes it asynchronously at a rate the database can safely handle.
3.  **Batch Ingestion**: Inserting rows in batches (`executemany` in the consumer) is significantly faster than opening a connection and committing a single row for every insert.
