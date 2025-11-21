-- View para análise mensal
CREATE OR REPLACE VIEW gold.monthly_sales_summary AS
SELECT
    year,
    month,
    SUM(total_sales) AS monthly_revenue,
    COUNT(*) AS transaction_count,
    AVG(total_sales) AS avg_transaction_value
FROM silver.sales_silver
GROUP BY year, month
ORDER BY year, month;

-- View para top produtos (com base no nome do produto)
CREATE OR REPLACE VIEW gold.top_products AS
SELECT
    product_name,
    SUM(total_sales) AS total_revenue
FROM silver.sales_silver
GROUP BY product_name
ORDER BY total_revenue DESC
LIMIT 10;
