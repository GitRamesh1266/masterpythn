import pandas as pd


# To perform NUll check on Transaction_id ,Transaction_date column and negative value present in transaction_amount
 
def Test_Nullcheck_Transaction_id():
    df2 = pd.DataFrame({'A': [1, 2,'nan'], 'B': [4, 5, 7]})
    Null_Check_Transaction_id=df2[['A','B']].isNull().any()
    return Null_Check_Transaction_id==False,'Transaction_id,Transaction_date column has null value - Please Verify'
     
print(Test_Nullcheck_Transaction_id)