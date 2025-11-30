# test_transform.py
import pandas as pd

def test_date_parsing():
    df = pd.DataFrame({
        'order_date': ['2025-10-10 12:00:00', '2025-11-01 00:00:00']
})
    df['order_date'] = pd.to_datetime(df['order_date'])
    assert df['order_date'].dtype == 'datetime64[ns]'
