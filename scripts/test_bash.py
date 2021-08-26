import pandas as pd 

def test_bash_command(data):
    df=pd.DataFrame(data)
    df.to_csv("/home/wittybrains/Desktop/csv_files/test_sc_bashcommand.csv",sep="|",index=False)


def main():
    data={"name":["abhinav","aakash"],
          "Id":[1,2]}
    test_bash_command(data)
    
          

if __name__=="__main__":
    main()