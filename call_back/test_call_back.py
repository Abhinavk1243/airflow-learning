import pandas as pd

def test_callback(context):
    d={"name":["A","B","C"]}
    df=pd.DataFrame(d)
    df.to_csv("/home/wittybrains/Desktop/csv_files/test_callback.csv")