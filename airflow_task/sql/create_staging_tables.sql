CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id INT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    signup_date DATE,
    country TEXT
);

CREATE TABLE IF NOT EXISTS stg_products (
    product_id INT,
    product_name TEXT,
    category TEXT,
    unit_price NUMERIC
);

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id TEXT,
    order_timestamp TIMESTAMP,
    customer_id INT,
    product_id INT,
    quantity INT,
    total_amount NUMERIC,
    currency TEXT,
    status TEXT
);
