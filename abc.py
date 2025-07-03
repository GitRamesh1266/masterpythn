import pandas as pd
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Load the Excel file
df = pd.read_excel("790b863f-40c6-45f2-862e-23e00e01eca2.xlsx")

# ---- TRANSFORMATIONS ----

# Convert 'transaction_date' to datetime
df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')

# Filter out invalid or negative 'amount' values
df = df[df['amount'].apply(lambda x: isinstance(x, (int, float)) and x >= 0)]

# ---- TESTS ----

def test_transaction_dates_are_valid():
    assert df['transaction_date'].notna().all(), "Some transaction_date values are invalid"
    logging.info("✅ All transaction_date values are valid.")

def test_amounts_are_non_negative():
    assert (df['amount'] >= 0).all(), "Some amounts are negative"
    logging.info("✅ All amount values are non-negative.")

def test_expected_columns():
    expected_cols = {'transaction_id', 'transaction_date', 'amount'}
    assert expected_cols.issubset(df.columns), "Missing expected columns"
    logging.info("✅ All expected columns are present.")

# ---- RUN TESTS ----

if __name__ == "__main__":
    try:
        test_transaction_dates_are_valid()
    except AssertionError as e:
        logging.error(f"❌ {e}")
        
    try:
        test_amounts_are_non_negative()
    except AssertionError as e:
        logging.error(f"❌ {e}")
        
    try:
        test_expected_columns()
    except AssertionError as e:
        logging.error(f"❌ {e}")