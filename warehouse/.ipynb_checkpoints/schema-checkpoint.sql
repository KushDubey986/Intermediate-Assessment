DROP TABLE IF EXISTS fact_transactions;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;

CREATE TABLE dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id INT,
    name VARCHAR(100),
    region VARCHAR(50),
    effective_from DATE,
    effective_to DATE,
    is_current BOOLEAN
);

CREATE TABLE dim_product (
    product_sk SERIAL PRIMARY KEY,
    product_id INT,
    product_name VARCHAR(100),
    category VARCHAR(50)
);

CREATE TABLE fact_transactions (
    transaction_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    amount DOUBLE PRECISION,
    transaction_date DATE,
    status VARCHAR(50),
    channel VARCHAR(50),
    avg_amount DOUBLE PRECISION,
    is_fraud BOOLEAN
);