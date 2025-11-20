# transform.py
import json
import pandas as pd
from sqlalchemy import create_engine, text

DB_URI = "postgresql+psycopg2://pguser:pgpass@localhost:5432/dd_project"

def raw_to_bronze(batch_size=100):
    engine = create_engine(DB_URI)
    with engine.begin() as conn:
        # obtém registros não processados (exemplo: todos)
        res = conn.execute(text("SELECT id, payload, received_at FROM raw.orders_raw ORDER BY id LIMIT :n"), {"n": batch_size})
        records = res.fetchall()
        if not records:
            print("No raw records to process.")
            return
        inserts = []
        for r in records:
            payload = json.loads(r.payload)
            inserts.append({
                "order_id": payload.get("order_id"),
                "customer_id": payload.get("customer_id"),
                "order_date": payload.get("order_date"),
                "total_amount": payload.get("total_amount"),
                "raw_received_at": r.received_at
            })
        # inserir em bronze (upsert simples)
        for i in inserts:
            conn.execute(
                text("""
                INSERT INTO bronze.orders (order_id, customer_id, order_date, total_amount, raw_received_at)
                VALUES (:order_id, :customer_id, :order_date, :total_amount, :raw_received_at)
                ON CONFLICT (order_id) DO NOTHING
                """), i)
    print(f"Moved {len(inserts)} rows raw -> bronze")

def bronze_to_silver():
    engine = create_engine(DB_URI)
    with engine.begin() as conn:
        df = pd.read_sql("SELECT * FROM bronze.orders", conn)
        if df.empty:
            print("No bronze rows")
            return
        # limpeza: converter tipos
        df['order_date'] = pd.to_datetime(df['order_date'])
        df['total_amount'] = pd.to_numeric(df['total_amount'])
        df['order_year'] = df['order_date'].dt.year
        df['order_month'] = df['order_date'].dt.month
        df['order_day'] = df['order_date'].dt.day
        # upsert into silver
        for _, row in df.iterrows():
            conn.execute(text("""
              INSERT INTO silver.orders_clean (order_id, customer_id, order_date, total_amount, order_year, order_month, order_day)
              VALUES (:order_id, :customer_id, :order_date, :total_amount, :order_year, :order_month, :order_day)
              ON CONFLICT (order_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                order_date = EXCLUDED.order_date,
                total_amount = EXCLUDED.total_amount,
                order_year = EXCLUDED.order_year,
                order_month = EXCLUDED.order_month,
                order_day = EXCLUDED.order_day
            """), {
                "order_id": row['order_id'],
                "customer_id": row['customer_id'],
                "order_date": row['order_date'],
                "total_amount": float(row['total_amount']),
                "order_year": int(row['order_year']),
                "order_month": int(row['order_month']),
                "order_day": int(row['order_day'])
            })
    print("Bronze -> Silver done")

if __name__ == "__main__":
    raw_to_bronze()
    bronze_to_silver()
