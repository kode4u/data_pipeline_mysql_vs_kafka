CREATE DATABASE IF NOT EXISTS warehouse_db;
USE warehouse_db;

-- 1. Customer Dimension
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNIQUE NOT NULL, -- Business key from source OLTP
    full_name VARCHAR(101) NOT NULL, -- concatenation of first_name and last_name
    country VARCHAR(50) NOT NULL,
    row_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Product Dimension
CREATE TABLE IF NOT EXISTS dim_products (
    product_key INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT UNIQUE NOT NULL, -- Business key from source OLTP
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    row_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Sales Fact Table
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL, -- Business key from source OLTP
    customer_key INT NOT NULL, -- Reference to dim_customers
    product_key INT NOT NULL, -- Reference to dim_products
    quantity INT NOT NULL,
    gross_amount DECIMAL(12, 2) NOT NULL,
    discount_amount DECIMAL(12, 2) NOT NULL,
    net_amount DECIMAL(12, 2) NOT NULL,
    order_date_hour INT NOT NULL, -- Format: YYYYMMDDHH for partitioned analytical query support
    row_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_products(product_key)
);
