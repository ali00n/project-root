# load.py
from sqlalchemy import create_engine, text

DB_URI = "postgresql+psycopg2://pguser:pgpass@localhost:5432/dd_project"

def build_monthly_revenue():
    engine = create_engine(DB_URI)
    with engine.begin() as conn:
        conn.execute(text("""
        INSERT INTO gold.monthly_revenue (year, month, revenue, orders_count)
        SELECT order_year as year,
               order_month as month,
               SUM(total_amount) as revenue,
               COUNT(*) as orders_count
        FROM silver.orders_clean
        GROUP BY order_year, order_month
        ON CONFLICT (year, month) DO UPDATE SET
          revenue = EXCLUDED.revenue,
          orders_count = EXCLUDED.orders_count;
        """))
    print("Gold monthly_revenue updated")

if __name__ == "__main__":
    build_monthly_revenue()
