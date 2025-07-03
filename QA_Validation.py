import pandas as pd
from pyspark.sql.functions import col, to_date
from datetime import datetime
import logging

logging.basicConfig(
    filename='QA_Log.log',  # Output file name
    level=logging.INFO,  # Minimum level to log
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format with timestamp
    datefmt='%Y-%m-%d %H:%M:%S'  # Timestamp format
)
# Load the Excel file
df = pd.read_csv("transaction_data.csv")
silver_df=pd.read_csv("silver_transaction_data.csv")
# ----Handling NUll values for transaction_id,transaction_date 
# with TRANSFORMATIONS for transaction_date and Filtering transaction_amount for negative amount


# columns_to_check = ['transaction_date', 'transaction_id']
# has_nulls = df[~df[columns_to_ check].isnull().any(axis=1)] #Verified the null records present for id and date column
# final_df= has_nulls[has_nulls['transaction_amount'].apply(lambda x: isinstance(x, (int, float)) and x >= 0)] # filtered the negative transaction_amount
# final_df.loc[:,'transaction_date']=pd.to_datetime(final_df['transaction_date'],format = "%d-%m-%Y") #Transformation applied on transaction_date column
# print("/---Final Result set after filter and transformation-/")
# print(final_df)


# ---- TESTS ----/

def test_incremental_row_count():
    new_rows = df[~df["transaction_id"].isin(silver_df["transaction_id"])]
    expected_new = len(new_rows)
    combined = pd.concat([silver_df, new_rows]).drop_duplicates(subset="transaction_id")
    actual_new = len(combined) - len(silver_df)
    assert actual_new == expected_new, f"Expected {expected_new}, got {actual_new} new rows"
    logging.info("Incremental record count are matching")
    
def test_overlapping():    
    overlapping = set(df["transaction_id"]).intersection(set(silver_df["transaction_id"]))
    assert len(overlapping) != 0, f"Duplicate transaction_ids found"
    logging.info("There i no overapping")
    
def test_duplicate_ingestion():
    initial_count = silver_df.shape[0]

# Perform deduplication logic (based on unique transaction_id)
# Combine both and drop duplicates to simulate idempotent ingestion
    combined_df = pd.concat([silver_df, df])
    combined_df = combined_df.drop_duplicates(subset=["transaction_id"])

    final_count = combined_df.shape[0]
    new_rows_added = final_count - initial_count

    assert new_rows_added == 0, f"Test failed: {new_rows_added} duplicate rows were added!"
    logging.info("✅ Duplicate ingestion test passed: No new rows added on re-ingestion.")
     

def test_max_transaction_date_for_incremental_load():
    df.loc[:, 'transaction_date']=pd.to_datetime(df['transaction_date'], format="%d-%m-%Y", errors='coerce')
    silver_df['transaction_date'] = pd.to_datetime(silver_df['transaction_date'], format="%d-%m-%Y", errors='coerce')
    raw_maxdate = final_df['transaction_date'].max()
    silver_maxdate = silver_df['transaction_date'].max()
    assert raw_maxdate >= silver_maxdate, "No incremental data"
    logging.info("Incremental records are present")

def test_transaction_date_and_Transaction_id_null():
    columns_to_check = ['transaction_date', 'transaction_id']
    has_nulls = df[columns_to_check].isnull().any(axis=1).sum()
    assert has_nulls == 0, "Null Records are present in transaction_date and transaction_id"
    logging.info("Records are present in transaction_date and transaction_id.")

def test_transaction_date_null_records()
    null_count=df["transaction_date"].isnull()
    
def test_Duplicate_Transaction_id():
    has_duplicate= df.duplicated(subset='transaction_id').sum()
    assert has_duplicate == 0, "Duplicate transaction_id present in file"
    logging.info("Unique Records are present in file.")


def Test_File_Empty_schema_column():
    required_columns ={"transaction_id","customer_id","transaction_amount","transaction_date","payment_method","status"}
    check_empty=not df.empty
    schema_column_check=required_columns.issubset(df.columns)
    # if schema_column_check:
    #     print('schea col check true')
    assert check_empty and schema_column_check,"Given file is Empty or having column mismatch"
    logging.info("File contain records and not empty and table structure like columns are matching")
    print("✅ File contain records and not empty and table structure like columns are matching")
    
def test_expected_columns():
    expected_cols = {'transaction_id', 'transaction_date', 'transaction_amount'}
    assert expected_cols.issubset(df.columns), "Missing expected columns"
    logging.info("✅ All expected columns are present.")
    print("✅ All expected columns are present")

# ---- RUN TESTS ----

if __name__ == "__main__":
    
     try:
         test_transaction_date_null_records()
     except AssertionError as e:
         logging.error(f"❌ {e}")
     
    # try:
    #     test_duplicate_ingestion()
    # except AssertionError as e:
    #     logging.error(f"❌ {e}")
    # try:
    #     test_overlapping()
    # except AssertionError as e:
    #     logging.error(f"❌ {e}")
    
    # try:
    #     test_incremental_row_count()
    # except AssertionError as e:
    #     logging.error(f"❌ {e}")
    
    # try:
    #     test_max_transaction_date_for_incremental_load()
    # except AssertionError as e:
    #     logging.error(f"❌ {e}")
    # try:
    #     Test_File_Empty_schema_column()
    # except AssertionError as e:
    #     logging.error(f"❌ {e}")
                 
    # try:
    #     test_Duplicate_Transaction_id()
    # except AssertionError as e:
    #     logging.error(f"❌ {e}")
         
    # try:
    #     test_transaction_date_and_Transaction_id_null()
    # except AssertionError as e:
    #     logging.error(f"❌ {e}")
   
    # try:
    #     test_expected_columns()
    # except AssertionError as e:
    #     logging.error(f"❌ {e}")
    