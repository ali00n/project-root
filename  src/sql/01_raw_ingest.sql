-- Criação de índices para otimização

-- Índices para a camada Bronze
CREATE INDEX IF NOT EXISTS idx_bronze_sales_date ON bronze.sales_bronze(date);
CREATE INDEX IF NOT EXISTS idx_bronze_sales_product ON bronze.sales_bronze(product_name);
CREATE INDEX IF NOT EXISTS idx_bronze_sales_customer ON bronze.sales_bronze(customer_id);

-- Índices para a camada Silver
CREATE INDEX IF NOT EXISTS idx_silver_sales_date ON silver.sales_silver(date);
CREATE INDEX IF NOT EXISTS idx_silver_sales_product ON silver.sales_silver(product_name);
CREATE INDEX IF NOT EXISTS idx_silver_sales_region ON silver.sales_silver(region);
CREATE INDEX IF NOT EXISTS idx_silver_sales_year_month ON silver.sales_silver(year, month);

-- Índices para a camada Gold
CREATE INDEX IF NOT EXISTS idx_gold_monthly_year_month ON gold.monthly_sales(year, month);
CREATE INDEX IF NOT EXISTS idx_gold_products_name ON gold.product_performance(product_name);
CREATE INDEX IF NOT EXISTS idx_gold_region_name ON gold.regional_sales(region);

-- Estatísticas para o otimizador de queries
ANALYZE bronze.sales_bronze;
ANALYZE silver.sales_silver;
ANALYZE gold.monthly_sales;
ANALYZE gold.product_performance;
ANALYZE gold.regional_sales;