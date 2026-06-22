import mysql.connector
import random
import time
from datetime import datetime, timedelta

# Database Connection (from host)
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'rootpassword',
    'port': 3307,
    'use_pure': True
}

USERS_SEED = [
    ("alice.smith@example.com", "Alice", "Smith", "USA"),
    ("bob.jones@example.com", "Bob", "Jones", "Canada"),
    ("charlie.brown@example.com", "Charlie", "Brown", "UK"),
    ("david.wilson@example.com", "David", "Wilson", "Germany"),
    ("eva.miller@example.com", "Eva", "Miller", "France"),
    ("frank.davis@example.com", "Frank", "Davis", "Australia"),
    ("grace.thomas@example.com", "Grace", "Thomas", "Japan"),
    ("henry.white@example.com", "Henry", "White", "Singapore"),
    ("ivy.harris@example.com", "Ivy", "Harris", "Cambodia"),
    ("jack.martin@example.com", "Jack", "Martin", "Thailand")
]

PRODUCTS_SEED = [
    ("Laptop Pro", "Electronics", 1200.00),
    ("Smartphone X", "Electronics", 800.00),
    ("Wireless Headphones", "Accessories", 150.00),
    ("Coffee Maker", "Appliances", 90.00),
    ("Leather Backpack", "Apparel", 120.00),
    ("Running Shoes", "Apparel", 85.00)
]

def init_schemas(cursor):
    print("Reading and applying source database schema...")
    with open("data_etl/sql/source_schema.sql", "r") as f:
        source_sql = f.read()
    
    # Split queries by semicolon to execute individually
    for statement in source_sql.split(";"):
        if statement.strip():
            cursor.execute(statement)

    print("Reading and applying warehouse database schema...")
    with open("data_etl/sql/warehouse_schema.sql", "r") as f:
        warehouse_sql = f.read()
        
    for statement in warehouse_sql.split(";"):
        if statement.strip():
            cursor.execute(statement)
            
    # Grant permissions to pipeline_user
    print("Granting database permissions to pipeline_user...")
    cursor.execute("GRANT ALL PRIVILEGES ON source_db.* TO 'pipeline_user'@'%';")
    cursor.execute("GRANT ALL PRIVILEGES ON warehouse_db.* TO 'pipeline_user'@'%';")
    cursor.execute("FLUSH PRIVILEGES;")

def seed_static_dimensions(conn, cursor):
    print("Seeding base tables (Users & Products)...")
    
    # Insert Users
    cursor.execute("USE source_db;")
    cursor.execute("SELECT COUNT(*) FROM users;")
    if cursor.fetchone()[0] == 0:
        cursor.executemany(
            "INSERT INTO users (email, first_name, last_name, country) VALUES (%s, %s, %s, %s)",
            USERS_SEED
        )
        conn.commit()
        print(f"Seeded {len(USERS_SEED)} users.")
    else:
        print("Users already seeded.")

    # Insert Products
    cursor.execute("SELECT COUNT(*) FROM products;")
    if cursor.fetchone()[0] == 0:
        cursor.executemany(
            "INSERT INTO products (name, category, price) VALUES (%s, %s, %s)",
            PRODUCTS_SEED
        )
        conn.commit()
        print(f"Seeded {len(PRODUCTS_SEED)} products.")
    else:
        print("Products already seeded.")

def seed_random_orders(conn, cursor, count=15):
    print(f"Generating {count} mock transactions over the last 3 hours...")
    
    # Fetch user ids and products
    cursor.execute("USE source_db;")
    cursor.execute("SELECT user_id FROM users;")
    user_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT product_id, price FROM products;")
    products = cursor.fetchall() # list of (product_id, price)
    
    from datetime import timezone
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    
    statuses = ['completed', 'completed', 'completed', 'pending', 'cancelled']
    
    for i in range(count):
        # Pick random customer and random time in the last 3 hours
        user_id = random.choice(user_ids)
        random_minutes = random.randint(0, 180)
        order_time = now - timedelta(minutes=random_minutes)
        status = random.choice(statuses)
        
        # Insert Order
        cursor.execute(
            "INSERT INTO orders (user_id, order_date, status) VALUES (%s, %s, %s)",
            (user_id, order_time.strftime('%Y-%m-%d %H:%M:%S'), status)
        )
        order_id = cursor.lastrowid
        
        # Insert 1-3 Order Items
        num_items = random.randint(1, 3)
        selected_products = random.sample(products, num_items)
        
        for prod_id, price in selected_products:
            quantity = random.randint(1, 3)
            # 10% chance of a discount
            discount = round(float(price) * 0.1, 2) if random.random() < 0.15 else 0.00
            
            cursor.execute(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount) VALUES (%s, %s, %s, %s, %s)",
                (order_id, prod_id, quantity, price, discount)
            )
            
    conn.commit()
    print(f"Successfully generated orders and order items.")

def main():
    print("Connecting to MySQL...")
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 1. Initialize schemas
        init_schemas(cursor)
        
        # 2. Seed static dimensions
        seed_static_dimensions(conn, cursor)
        
        # 3. Seed random orders
        seed_random_orders(conn, cursor)
        
        print("\n[SUCCESS] Schema initialized and mock database seeded successfully!")
        
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == '__main__':
    main()
