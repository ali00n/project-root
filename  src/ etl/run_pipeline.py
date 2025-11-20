# run_pipeline.py
from extract import ingest_csv_to_raw
from transform import raw_to_bronze, bronze_to_silver
from load import build_monthly_revenue

def run_all():
    ingest_csv_to_raw()
    raw_to_bronze()
    bronze_to_silver()
    build_monthly_revenue()

if __name__ == "__main__":
    run_all()
