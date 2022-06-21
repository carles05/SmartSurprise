from urllib.request import urlopen
import json
import pandas as pd
from datetime import date
today = date.today()
import requests
import time

#Nos Conectamos a la BBDD
from sqlalchemy import create_engine
import urllib

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

symbol_list_2=symbol_list_df[0:14162]




#DECLARAMOS TODOS LOS DF INICIALES




ticker_list=symbol_list_2["symbol"]
peers_list_df=pd.DataFrame()
#EMPEZAMOS EL BUCLE GLOBAL DONDE PARA CADA UNO DE LOS TICKERS HARÁ TODOS LOS CALCULOS
for a in range(len(ticker_list)):
    print(a)
    base_url = 'https://finnhub.io/api/v1/stock/peers?symbol='    
    ticker_url=ticker_list.iloc[a] #empezar por 2094,
    end_url = '&token=brag3f7rh5rbgnjptbc0'
    ticker_req = base_url + ticker_url+end_url
    #Hacemos la get de peers
    r = requests.get(ticker_req)
    peers=r.json()
    peers_df = pd.DataFrame(peers) 
    peers_df = peers_df.transpose()
    peers_df["symbol"]=ticker_url
    peers_list_df=peers_list_df.append(peers_df)
    time.sleep(1)
    

#CAMBIAMOS LOS NOMBRES DE LA TABLA
peers_list_df = peers_list_df.rename(columns={0: 'Peer_1', 1: 'Peer_2',2: 'Peer_3', 3: 'Peer_4',
                                            4: 'Peer_5', 5: 'Peer_6',6: 'Peer_7', 7: 'Peer_7'
                                            ,8: 'Peer_9', 9: 'Peer_10'})

peers_list_df.to_sql('M_Peers_list', con = engine, if_exists = 'append',index=False)
Statement=pd.read_sql_table('ST_Financial_Growth',con=engine)


dataTypeSeries = Statement.dtypes

Statement['revenueGrowth']=Statement['revenueGrowth'].astype(float)
