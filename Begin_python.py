
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import logging

data = [("Alice", 30), ("Bob", None), ("Charlie", 25)]
df = spark.createDataFrame(data, ["name", "age"])

    transformed_df = handle_missing_values(df, "age", 0)
    result = transformed_df.filter(transformed_df.age.isNull()).count()

    assert result == 0, "Missing values were not handled correctly"