import pandas as pd
from pyspark.sql.functions import col, to_date
from datetime import datetime
import logging

# Load the Excel file
df = pd.read_csv("transaction_data.csv")

# ----Handling NUll values for transaction_id,transaction_date 
# with TRANSFORMATIONS for transaction_date and Filtering transaction_amount for negative amount


columns_to_check = ['transaction_date', 'transaction_id']
has_nulls = df[~df[columns_to_check].isnull().any(axis=1)] #Verified the null records present for id and date column
final_df= has_nulls[has_nulls['transaction_amount'].apply(lambda x: isinstance(x, (int, float)) and x >= 0)] # filtered the negative transaction_amount
final_df.loc[:,'transaction_date']=pd.to_datetime(final_df['transaction_date'],format = "%d-%m-%Y") #Transformation applied on transaction_date column
print("/---Final Result set after filter and transformation-/")
print(final_df)


# ---- TESTS ----

def Test_File_Empty_schema_column():
    required_columns ={"transaction_id","customer_id","transaction_amount","transaction_date","payment_method","status"}
    check_empty=not df.empty
    schema_column_check=required_columns.issubset(df.columns)
    assert check_empty & schema_column_check==True,"Given file is Empty or having column mismatch"
    logging.info("File contain records and table structure like columns are matching")

def test_transaction_date_and_Transaction_id_null():
    columns_to_check = ['transaction_date', 'transaction_id']
    has_nulls = df[columns_to_check].isnull().any(axis=1).sum()
    assert has_nulls == 0, "Null Records are present in transaction_date and transaction_id"
    logging.info("Records are present in transaction_date and transaction_id.")


def test_Duplicate_Transaction_id():
    has_duplicate= df.duplicated(subset='transaction_id').sum()
    assert has_duplicate == 0, "Duplicate transaction_id present in file"
    logging.info("Unique Records are present in file.")

#def test_transaction_dates_are_valid():
#    assert df['transaction_date'].notna().all(), "Some transaction_date values are invalid"
#   logging.info("✅ All transaction_date values are valid.")

def test_amounts_are_non_negative():
    assert (df['transaction_amount'] >= 0).all(), "Some amounts are negative"
    logging.info("✅ All amount values are non-negative.")

def Test_File_Empty_schema_column():
    required_columns ={"transaction_id","customer_id","transaction_amount","transaction_date","payment_method","status"}
    check_empty=not df.empty
    schema_column_check=required_columns.issubset(df.columns)
    assert check_empty & schema_column_check==True,"Given file is Empty or having column mismatch"
    logging.info("File contain records and not empty and table structure like columns are matching")
    
def test_expected_columns():
    expected_cols = {'transaction_id', 'transaction_date', 'transaction_amount'}
    assert expected_cols.issubset(df.columns), "Missing expected columns"
    logging.info("✅ All expected columns are present.")

# ---- RUN TESTS ----

if __name__ == "__main__":
    
    try:
        Test_File_Empty()
    except AssertionError as e:
        logging.error(f"❌ {e}")
    
    try:
        Test_File_Empty_schema_column()
    except AssertionError as e:
        logging.error(f"❌ {e}")
                 
    try:
        test_Duplicate_Transaction_id()
    except AssertionError as e:
        logging.error(f"❌ {e}")
         
    try:
        test_transaction_date_and_Transaction_id_null()
    except AssertionError as e:
        logging.error(f"❌ {e}")
        
    #try:
    #    test_transaction_dates_are_valid()
    #except AssertionError as e:
    #    logging.error(f"❌ {e}")
   
   