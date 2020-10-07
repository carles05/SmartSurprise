"Extract Marketcap from each Company"

from urllib.request import urlopen
import json
import pandas as pd
from datetime import date
today = date.today()
from sqlalchemy import create_engine
import urllib

params = urllib.parse.quote_plus("Driver={ODBC Driver 17 for SQL Server};Server=tcp:finbainservidor.database.windows.net,1433;Database=FinbainBBDD;Uid=finbainbcn;Pwd=Finbain2020BCN;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True)
connection = engine.connect()
    

def get_jsonparsed_data(url):
    """
    Receive the content of ``url``, parse it as JSON and return the object.

    Parameters
    ----------
    url : str

    Returns
    -------
    dict
    """
    response = urlopen(url)
    data = response.read().decode("utf-8")
    return json.loads(data)







#GET SYMBOL LIST
url = ("https://financialmodelingprep.com/api/v3/stock/list?apikey=b05bd33f2e28209bc4770d5ad2d20ace")


symbol_list=(get_jsonparsed_data(url))




#Hacemos un bucle para correr todos los tickers que hemos pedido
symbol_list_2= symbol_list[:]
n=len(symbol_list_2)
symbol_list_df=pd.DataFrame()

for i in range(n): 
    try:
        symbol_list_aux=symbol_list_2[i]
        exchange=symbol_list_aux["exchange"]
        name=symbol_list_aux["name"]
        symbol= symbol_list_aux["symbol"]
    
        df = pd.DataFrame({'exchange': [exchange], 'name': [name],'symbol': [symbol]})
    
        symbol_list_df=symbol_list_df.append(df)
    except:
        pass
    



#FILTRAMOS LO QUE YA HEMOS EXTRAIDO

symbol_list_2=symbol_list_df[0:len(symbol_list_df)] #desde TCS



ticker_list=symbol_list_2["symbol"]


for a in range(len(ticker_list)):
    try:
        

        print(a)
        #Filtramos la tabla referencia date para el symbol a
        #DEFINE                               
        base_url = ' https://fmpcloud.io/api/v3/profile/'
        ticker_url=ticker_list.iloc[a] #CFFAU 8600 APROX,
        end_url = '?apikey=27b5adb17295244f3695edd1c6605542'
        ticker_req = base_url + ticker_url+end_url
        profile=(get_jsonparsed_data(ticker_req))
        profile=pd.DataFrame(profile)
        profile=profile[['symbol','sector','industry','ceo','description','mktCap']]
        profile.to_sql('M_Symbol_Profile', con = engine, if_exists = 'append',index=False)
    except:
        pass
        
