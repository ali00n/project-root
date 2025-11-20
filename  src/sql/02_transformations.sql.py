-- Faturamento total
SELECT SUM(revenue) as total_revenue FROM gold.monthly_revenue;

-- Faturamento por mês
SELECT year, month, revenue FROM gold.monthly_revenue ORDER BY year, month;

-- Top 10 clientes por gasto
SELECT customer_id, SUM(total_amount) as spent
FROM silver.orders_clean
GROUP BY customer_id
ORDER BY spent DESC
LIMIT 10;

-- Tickets médios por mês
SELECT year, month, revenue::numeric / NULLIF(orders_count,0) as avg_ticket
FROM gold.monthly_revenue
ORDER BY year, month;
