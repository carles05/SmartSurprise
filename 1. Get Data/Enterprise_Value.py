"Extract historical Price"

from urllib.request import urlopen
import json
import pandas as pd
from datetime import date
today = date.today()
    


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


#Nos Conectamos a la BBDD
from sqlalchemy import create_engine
import urllib



params = urllib.parse.quote_plus("Driver={ODBC Driver 17 for SQL Server};Server=tcp:finbainservidor.database.windows.net,1433;Database=FinbainBBDD;Uid=finbainbcn;Pwd=Finbain2020BCN;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True)
connection = engine.connect()








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
symbol_list_2=symbol_list_df[9392:10000]




#DECLARAMOS TODOS LOS DF INICIALES






ticker_list=symbol_list_2["symbol"]
#EMPEZAMOS EL BUCLE GLOBAL DONDE PARA CADA UNO DE LOS TICKERS HARÁ TODOS LOS CALCULOS
for a in range(len(ticker_list)):

    print(a)       
    #DEFINE
    base_url = 'https://fmpcloud.io/api/v3/enterprise-values/'
    ticker_url=ticker_list.iloc[a] #empezar por 2094,
    end_url = '?period=quarter&apikey=27b5adb17295244f3695edd1c6605542'

    #GET TICKER
    ticker_req = base_url + ticker_url+end_url

    #Enterprise Value
    url_enterprise_value  = (ticker_req)
    enterprise_value=(get_jsonparsed_data(url_enterprise_value))
    
    
    #Hacemos un bucle para correr todos los tickers que hemos pedido
    n=len(enterprise_value)
    enterprise_value_df=pd.DataFrame()
    for b in range(n): 
        try:
            
            enterprise_value_aux=enterprise_value[b]
            
            symbol= ticker_url
            
            enterprise_df=pd.DataFrame.from_dict(enterprise_value_aux, orient='index')
            enterprise_df = enterprise_df.transpose()
            enterprise_df["symbol"] = symbol
            enterprise_df["extraction_date"]=today 
            enterprise_value_df=enterprise_value_df.append(enterprise_df)
        except:
            pass
            
    try:
        enterprise_value_df.to_sql('ST_Enterprise_Value', con = engine, if_exists = 'append',index=False)
    except:
        pass 