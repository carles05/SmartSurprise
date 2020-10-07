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








#DECLARAMOS TODOS LOS DF INICIALES
#Cogemos una tabla de muestra para ver que fechas tiene
date_df=pd.read_sql_table('ST_Balance_Sheet',con=engine)
#Convertimos la fecha a date
date_df['date']=pd.to_datetime(date_df['date'],errors='coerce')
#Nos quedamos con lso campos necesarios
date_df=date_df[['date','symbol']]

#Ahora leemos la masterdata de fechas
date_masterdata=pd.read_sql_table('M_Date',con=engine)
date_masterdata['date']=date_masterdata['Date']



ticker_list=symbol_list_2["symbol"]
#EMPEZAMOS EL BUCLE GLOBAL DONDE PARA CADA UNO DE LOS TICKERS HARÁ TODOS LOS CALCULOS
for a in range(len(ticker_list)):
    try:
        

        print(a)
        #Filtramos la tabla referencia date para el symbol a
        date_df_aux = date_df[(date_df['symbol'] == ticker_list.iloc[a])]
        #Datos de Marketcap
        #DEFINE
        base_url = 'https://fmpcloud.io/api/v3/historical-market-capitalization/'
        ticker_url=ticker_list.iloc[a] #CFFAU 8600 APROX,
        end_url = '?period=quarter&apikey=27b5adb17295244f3695edd1c6605542'
        ticker_req = base_url + ticker_url+end_url
        marketcap=(get_jsonparsed_data(ticker_req))
        marketcap_df=pd.DataFrame(marketcap)
        marketcap_df["extraction_date"]=today
        #Pasamos fecha a date
        marketcap_df['date']=pd.to_datetime(marketcap_df['date'],errors='coerce')
        marketcap_df_aux=pd.merge(date_masterdata,marketcap_df,how='left',on='date')
        marketcap_df_aux['marketCap'].fillna(method='ffill', inplace=True)
        marketcap_df_aux['symbol'].fillna(method='ffill', inplace=True)
        #Finalmente filtramos con date_df para quedarnos con el marketcap
        marketcap_df_aux_2=pd.merge(date_df_aux,marketcap_df_aux,how='left',on='date')
        marketcap_df_aux_2['symbol']=marketcap_df_aux_2['symbol_y']
        marketcap_df_aux_2=marketcap_df_aux_2[['date','symbol','marketCap']]
        
        #Lo enviamos a la BBDD
        marketcap_df_aux_2.to_sql('ST_Marketcap', con = engine, if_exists = 'append',index=False)
    except:
        pass

           

    

    

    

    