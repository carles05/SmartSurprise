"Extract historical Price"

from urllib.request import urlopen
import json
import pandas as pd

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
    
    
    
    
    
    
    
    
ticker_list=symbol_list_df["symbol"] 
    



#Hacemos un bucle para correr todos los tickers que hemos pedido


    
    
    
    
    
    
earning_calendar_df=pd.DataFrame()    
    
    
    
for a in range(len(ticker_list)):

    print(a)       
    #DEFINE
    base_url = ' https://fmpcloud.io/api/v3/historical/earning_calendar/'
    
    ticker_url=ticker_list.iloc[a] #empezar por 2094,
    end_url = '?apikey=27b5adb17295244f3695edd1c6605542'
    
    #GET TICKER
    ticker_req = base_url + ticker_url+end_url
    
    #Enterprise Value
    
    earning=(get_jsonparsed_data(ticker_req))
    
    
    #Hacemos un bucle para correr todos los tickers que hemos pedido
    n=len(earning)
    for b in range(n): 
        try:
            
            earning_aux=earning[b]
            
            
            earning_df=pd.DataFrame.from_dict(earning_aux, orient='index')
            earning_df = earning_df.transpose()
            earning_df["Surprise"]=earning_df["eps"]-earning_df["epsEstimated"]
            earning_calendar_df=earning_calendar_df.append(earning_df)
        except:
            pass
        
earning_calendar_df.to_csv("earning_calendar_1.csv") 


earning=pd.read_csv(r'C:\Users\Adrià\earning_calendar_1.csv)

