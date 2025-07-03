# validate_transactions.py
import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import logging


# Setup Logging
#logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Start Spark Session
#spark = SparkSession.builder.appName("TransactionDataValidation").getOrCreate()

# Load CSV
#df = spark.read.option("header", True).csv("transaction_data.csv")

# to check if duplicate transaction_id present into file
def Test_CheckDuplicates():
    df = pd.read_csv("transaction_data.csv")
    duplicate_count=pd.duplicated(subset='transaction_id').sum()
    assert duplicate_count==0,'Duplicate transation_id present in file - Please Verify'
    

# To perform NUll check on Transaction_id ,Transaction_date column and negative value present in transaction_amount
 
def Test_Nullcheck_Transaction_id():
    df = pd.read_csv("transaction_data.csv")
    Null_Check_Transaction_id=df[['transaction_id','transaction_date']].isNull().any()
    assert Null_Check_Transaction_id==False,'Transaction_id,Transaction_date column has null value - Please Verify'
    