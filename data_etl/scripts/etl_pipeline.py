import argparse
import mysql.connector
from datetime import datetime, timedelta
import sys

# Database Configuration Base
DB_CONFIG_BASE = {
    'user': 'pipeline_user',
    'password': 'pipeline_password',
    'port': 3307,
    'use_pure': True
}

def get_connection(host):
    # Airflow services run inside the docker network, so they connect on port 3306.
    # Script running on host connects on port 3307.
    port = 3306 if host != 'localhost' else 3307
    config = DB_CONFIG_BASE.copy()
    config['host'] = host
    config['port'] = port
    return mysql.connector.connect(**config)

def extract_and_load_dimensions(conn, cursor):
    print("--- 1. Extract & Load Dimensions (SCD Type 1 Upsert) ---")
    
    # --- dim_customers ---
    # Extract users from source_db
    cursor.execute("USE source_db;")
    cursor.execute("SELECT user_id, first_name, last_name, country FROM users;")
    users = cursor.fetchall()
    
    # Load into warehouse_db.dim_customers
    cursor.execute("USE warehouse_db;")
    inserted_users = 0
    for user_id, first_name, last_name, country in users:
        full_name = f"{first_name} {last_name}"
        # SCD Type 1: Insert new or overwrite existing fields
        query = """
            INSERT INTO dim_customers (user_id, full_name, country)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                full_name = VALUES(full_name),
                country = VALUES(country);
        """
        cursor.execute(query, (user_id, full_name, country))
        inserted_users += 1
    
    print(f"Upserted {inserted_users} rows into dim_customers.")
    
    # --- dim_products ---
    # Extract products from source_db
    cursor.execute("USE source_db;")
    cursor.execute("SELECT product_id, name, category FROM products;")
    products = cursor.fetchall()
    
    # Load into warehouse_db.dim_products
    cursor.execute("USE warehouse_db;")
    inserted_products = 0
    for product_id, name, category in products:
        query = """
            INSERT INTO dim_products (product_id, name, category)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                category = VALUES(category);
        """
        cursor.execute(query, (product_id, name, category))
        inserted_products += 1
        
    print(f"Upserted {inserted_products} rows into dim_products.")
    conn.commit()

def run_fact_etl(conn, cursor, start_time, end_time):
    print(f"--- 2. Extract, Transform & Load Fact Table ({start_time} to {end_time}) ---")
    
    # Extract orders and items in time range from source_db
    cursor.execute("USE source_db;")
    query_extract = """
        SELECT 
            o.order_id,
            o.user_id,
            o.order_date,
            oi.product_id,
            oi.quantity,
            oi.unit_price,
            oi.discount
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        WHERE o.order_date >= %s AND o.order_date < %s
          AND o.status = 'completed';
    """
    cursor.execute(query_extract, (start_time, end_time))
    raw_facts = cursor.fetchall()
    
    if not raw_facts:
        print("No completed orders found in this time window. Fact loading skipped.")
        return
    
    print(f"Extracted {len(raw_facts)} completed order line items from source.")
    
    # Fetch dimension surrogate keys mapping (Business key -> Surrogate Key)
    cursor.execute("USE warehouse_db;")
    
    cursor.execute("SELECT user_id, customer_key FROM dim_customers;")
    customer_map = dict(cursor.fetchall())
    
    cursor.execute("SELECT product_id, product_key FROM dim_products;")
    product_map = dict(cursor.fetchall())
    
    # Transform: Calculate metrics and map keys
    transformed_records = []
    order_ids_to_clean = set()
    
    for order_id, user_id, order_date, product_id, quantity, unit_price, discount in raw_facts:
        customer_key = customer_map.get(user_id)
        product_key = product_map.get(product_id)
        
        if not customer_key or not product_key:
            print(f"[WARNING] Missing dimension key mapping for user_id={user_id} or product_id={product_id}. Skipping record.")
            continue
            
        # Metrics transformation logic
        gross_amount = quantity * float(unit_price)
        discount_amount = quantity * float(discount)
        net_amount = gross_amount - discount_amount
        
        # Partition column: YYYYMMDDHH
        order_date_hour = int(order_date.strftime("%Y%m%d%H"))
        
        transformed_records.append((
            order_id,
            customer_key,
            product_key,
            quantity,
            gross_amount,
            discount_amount,
            net_amount,
            order_date_hour
        ))
        order_ids_to_clean.add(order_id)
        
    if not transformed_records:
        print("No valid records to load after transformation.")
        return

    # Idempotent Load: Delete existing data for the orders we are loading (so re-runs don't duplicate rows)
    # Convert set to string list for SQL IN operator
    order_id_list_str = ",".join(str(oid) for oid in order_ids_to_clean)
    cursor.execute(f"DELETE FROM fact_sales WHERE order_id IN ({order_id_list_str});")
    print(f"Cleaned up existing fact records for orders: {order_id_list_str} to maintain idempotence.")
    
    # Insert new fact records
    query_insert = """
        INSERT INTO fact_sales (
            order_id, customer_key, product_key, quantity, gross_amount, discount_amount, net_amount, order_date_hour
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.executemany(query_insert, transformed_records)
    conn.commit()
    print(f"Successfully loaded {len(transformed_records)} records into fact_sales.")

def run_data_quality_checks(cursor):
    print("--- 3. Run Data Quality Checks ---")
    cursor.execute("USE warehouse_db;")
    
    # Check 1: Null check on keys
    cursor.execute("""
        SELECT COUNT(*) 
        FROM fact_sales 
        WHERE customer_key IS NULL OR product_key IS NULL OR sales_key IS NULL;
    """)
    null_keys_count = cursor.fetchone()[0]
    if null_keys_count > 0:
        print(f"[DQ ERROR] Found {null_keys_count} records in fact_sales with NULL keys!")
        sys.exit(1)
    else:
        print("[DQ CHECK PASSED] No NULL surrogate keys in fact_sales.")
        
    # Check 2: Financial logic validation
    # Net amount should equal gross amount minus discount amount (allowing small float tolerances)
    cursor.execute("""
        SELECT COUNT(*) 
        FROM fact_sales 
        WHERE ABS(net_amount - (gross_amount - discount_amount)) > 0.01;
    """)
    failed_logic_count = cursor.fetchone()[0]
    if failed_logic_count > 0:
        print(f"[DQ ERROR] Found {failed_logic_count} records where Net != Gross - Discount!")
        sys.exit(1)
    else:
        print("[DQ CHECK PASSED] Financial metrics logic validation passed.")

def main():
    parser = argparse.ArgumentParser(description="E-commerce Batch ETL Job.")
    parser.add_argument("--host", type=str, default="localhost", help="MySQL Database host ('localhost' or 'mysql')")
    parser.add_argument("--start_time", type=str, help="Start timestamp in format YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--end_time", type=str, help="End timestamp in format YYYY-MM-DD HH:MM:SS")
    args = parser.parse_args()
    
    # Default to past 24 hours if time window is not specified
    if not args.start_time or not args.end_time:
        end = datetime.now()
        start = end - timedelta(hours=24)
        start_time_str = start.strftime("%Y-%m-%d %H:%M:%S")
        end_time_str = end.strftime("%Y-%m-%d %H:%M:%S")
    else:
        start_time_str = args.start_time
        end_time_str = args.end_time
        
    print(f"Initializing ETL connection to host: {args.host}")
    
    try:
        conn = get_connection(args.host)
        cursor = conn.cursor()
        
        # Phase 1: Dimensions SCD Type 1 Upsert
        extract_and_load_dimensions(conn, cursor)
        
        # Phase 2: Fact Ingestion
        run_fact_etl(conn, cursor, start_time_str, end_time_str)
        
        # Phase 3: Data Quality Checks
        run_data_quality_checks(cursor)
        
        print("\n[SUCCESS] ETL Pipeline completed successfully with data quality checks passed!")
        
    except mysql.connector.Error as err:
        print(f"Database Error: {err}")
        sys.exit(1)
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == '__main__':
    main()
