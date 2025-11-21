-- Exemplo de transformações SQL adicionais
-- View para análise mensal
CREATE OR REPLACE VIEW gold.monthly_sales_summary AS
SELECT
    year,
    month,
    SUM(total_sales) as monthly_revenue,
    COUNT(*) as transaction_count,
    AVG(total_sales) as avg_transaction_value
FROM silver.silver_sales
GROUP BY year, month
ORDER BY year, month;

-- View para top produtos (se existirem dados de produtos)
CREATE OR REPLACE VIEW gold.top_products AS
SELECT
    product_id,
    product_name,
    SUM(total_sales) as total_revenue
FROM silver.silver_sales
GROUP BY product_id, product_name
ORDER BY total_revenue DESC
LIMIT 10;