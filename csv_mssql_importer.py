from sqlalchemy import create_engine
import urllib
import pandas as pd 


db_instance_name = r'ZUNA-LAPTOP'
db_name = 'DataLakesDB'
new_db_name = 'Insurance'

path = r"C:\Users\T440P\Downloads\People.csv"


def Data_Import():

   

    quoted = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+db_instance_name+';DATABASE='+db_name+';Trusted_Connection=yes')
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

    df = pd.read_csv(path,engine='python',index_col=False)

    df.to_sql('zuna',schema='dbo',con=engine,if_exists='replace',index=False)
    print('Done with import')


Data_Import()
