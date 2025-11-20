# extract.py
import csv
import json
from sqlalchemy import create_engine, text

DB_URI = "postgresql+psycopg2://pguser:pgpass@localhost:5432/dd_project"

def ingest_csv_to_raw(csv_path="datasets/sample_data.csv"):
    engine = create_engine(DB_URI)
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = [json.dumps(row) for row in reader]

    with engine.begin() as conn:
        for r in rows:
            conn.execute(text("INSERT INTO raw.orders_raw (payload) VALUES (:p)"), {"p": r})
    print(f"Ingested {len(rows)} rows into raw.orders_raw")

if __name__ == "__main__":
    ingest_csv_to_raw()
