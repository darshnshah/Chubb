CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    signup_date DATE,
    country TEXT
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id INT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    unit_price NUMERIC
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id TEXT,
    order_timestamp_utc TIMESTAMP,
    order_date DATE,
    customer_id INT,
    product_id INT,
    quantity INT,
    total_amount_usd NUMERIC,
    currency TEXT,
    currency_mismatch_flag BOOLEAN,
    status TEXT
);
