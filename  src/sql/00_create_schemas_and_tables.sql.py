import NOT

-- Cria schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Exemplo: vamos supor dados de vendas (vendas online)
-- Tabela raw: armazenamento tal qual recebido (JSON/text)
CREATE TABLE IF NOT EXISTS raw.orders_raw (
  id SERIAL PRIMARY KEY,
  received_at TIMESTAMP DEFAULT now(),
  payload JSONB
);

-- Bronze: descompacta campos essenciais (estrutura base)
CREATE TABLE IF NOT EXISTS bronze.orders (
  order_id TEXT PRIMARY KEY,
  customer_id TEXT,
  order_date TIMESTAMP,
  total_amount NUMERIC,
  raw_received_at TIMESTAMP
);

-- Silver: dados com tipos definidos, limpeza e atributos calculados
CREATE TABLE IF NOT EXISTS silver.orders_clean (
  order_id TEXT PRIMARY KEY,
  customer_id TEXT,
  order_date TIMESTAMP,
  total_amount NUMERIC,
  order_year INT,
  order_month INT,
  order_day INT
);

-- Gold: agregações/KPIs (exemplo: faturamento por mês)
CREATE TABLE IF NOT EXISTS gold.monthly_revenue (
  year INT,
  month INT,
  revenue NUMERIC,
  orders_count INT,
  PRIMARY KEY (year, month)
);
