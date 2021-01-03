import requests
import datetime
import json
from sqlalchemy import create_engine
import urllib
import pandas as pd 



current = datetime.datetime.now()
formatted_date = str(current.year)+''+str(current.month)+''+str(current.day)
db_instance_name = 'ZUNA-LAPTOP'
db_name = 'DataLakesDB'
folderpath = "C:\\Users\\T440P\\Downloads\\"
filename = ('%speople.%s.csv'%(folderpath,formatted_date))

def api_data_pull():

    response = requests.get("http://api.open-notify.org/astros.json")

    people = response.json()

   

    Dict = {}
    
    
    for i in people['people']:
        names = i['name']

        splitname = names.split(" ")
        firstname = splitname[0]
        lastname = splitname[1]
    
        partition = ('People/year%s/month%s/day%s'%(str(current.year),str(current.month),str(current.day)))

        if Dict.get(partition,'Missing')=='Missing':
                Dict[partition] = "first_name,last_name\n"
        
        Dict[partition] += \
        '"'+str(firstname)+'",' \
        '"'+str(lastname)+'"\n'

        with open(filename,"w") as f:
                for partkey in Dict:
                    f.write(Dict[partkey])
                    print('Successfully saved to csv')

def db_data_insert():

    csv_name = filename
    table_name = "zunaid"

    quoted = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+db_instance_name+';DATABASE='+db_name+';Trusted_Connection=yes')
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

    df = pd.read_csv(csv_name,engine='python',index_col=False)

    df.to_sql(table_name,schema='dbo',con=engine,if_exists='replace',index=False)
    print('Done with import')


api_data_pull()
db_data_insert()