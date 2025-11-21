-- Criar schemas (se ainda não existirem)
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Tabelas Bronze
CREATE TABLE IF NOT EXISTS bronze.sales_bronze (
    id SERIAL PRIMARY KEY,
    date DATE,
    product_name TEXT,
    customer_id INT,
    amount NUMERIC
);

-- Tabelas Silver
CREATE TABLE IF NOT EXISTS silver.sales_silver (
    id SERIAL PRIMARY KEY,
    date DATE,
    product_name TEXT,
    region TEXT,
    year INT,
    month INT,
    amount NUMERIC
);

-- Tabelas Gold
CREATE TABLE IF NOT EXISTS gold.monthly_sales (
    id SERIAL PRIMARY KEY,
    year INT,
    month INT,
    total_sales NUMERIC
);

CREATE TABLE IF NOT EXISTS gold.product_performance (
    id SERIAL PRIMARY KEY,
    product_name TEXT,
    total_sales NUMERIC
);

CREATE TABLE IF NOT EXISTS gold.regional_sales (
    id SERIAL PRIMARY KEY,
    region TEXT,
    total_sales NUMERIC
);

-- Agora sim você pode criar índices e rodar ANALYZE
CREATE INDEX IF NOT EXISTS idx_bronze_sales_date ON bronze.sales_bronze(date);
CREATE INDEX IF NOT EXISTS idx_bronze_sales_product ON bronze.sales_bronze(product_name);
CREATE INDEX IF NOT EXISTS idx_bronze_sales_customer ON bronze.sales_bronze(customer_id);

CREATE INDEX IF NOT EXISTS idx_silver_sales_date ON silver.sales_silver(date);
CREATE INDEX IF NOT EXISTS idx_silver_sales_product ON silver.sales_silver(product_name);
CREATE INDEX IF NOT EXISTS idx_silver_sales_region ON silver.sales_silver(region);
CREATE INDEX IF NOT EXISTS idx_silver_sales_year_month ON silver.sales_silver(year, month);

CREATE INDEX IF NOT EXISTS idx_gold_monthly_year_month ON gold.monthly_sales(year, month);
CREATE INDEX IF NOT EXISTS idx_gold_products_name ON gold.product_performance(product_name);
CREATE INDEX IF NOT EXISTS idx_gold_region_name ON gold.regional_sales(region);

ANALYZE bronze.sales_bronze;
ANALYZE silver.sales_silver;
ANALYZE gold.monthly_sales;
ANALYZE gold.product_performance;
ANALYZE gold.regional_sales;
