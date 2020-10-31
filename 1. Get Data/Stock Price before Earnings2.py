from urllib.request import urlopen
import json
import pandas as pd



from urllib.request import urlopen
import json
import pandas as pd
from datetime import date
from datetime import datetime  
from datetime import timedelta  
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



#Funcion que usaremos para ver la evolución  de precios en los n dias anteriores y posteriores

def add_days(earnings_df_aux,day):

    earnings_df_aux_30= earnings_df_aux['date'] + timedelta(days=day) 
    earnings_df_aux_30=pd.DataFrame(earnings_df_aux_30)
    earnings_df_aux_30['cruce']=earnings_df_aux_30.index
    earnings_df_aux_30['year'] = pd.DatetimeIndex(earnings_df_aux_30['date']).year
    earnings_df_aux_30['month'] = pd.DatetimeIndex(earnings_df_aux_30['date']).month
    earnings_df_aux_30['day'] = pd.DatetimeIndex(earnings_df_aux_30['date']).day
    return earnings_df_aux_30


def define_q(date):
    if date.month<4:
        return pd.to_datetime(str(date.year-1)+'-12-31')
    elif date.month<7:
        return pd.to_datetime(str(date.year)+'-03-31')
    elif date.month<10:
        return pd.to_datetime(str(date.year)+'-06-30')
    else:
        return pd.to_datetime(str(date.year)+'-09-30')



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
    



#DECLARAMOS TODOS LOS DF INICIALES
#Cogemos una tabla de muestra para ver que fechas tiene
earnings_df=pd.read_sql_table('ST_Earnings',con=engine)







#FILTRAMOS LO QUE YA HEMOS EXTRAIDO

symbol_list_2=symbol_list_df[20000:] 

ticker_list=symbol_list_2["symbol"]

#Cruzamos con SP500 para quedarnos solo con los tickers de sp500
#SP_500=pd.read_sql_table('M_SP500_list',con=engine)
#SP_500.head()
#SP_500['symbol']=SP_500['Symbol']
#ticker_list=pd.merge(ticker_list,SP_500,how='inner',on='symbol')
#ticker_list=ticker_list["symbol"]


earnings_analysis=pd.DataFrame()

for a in range(len(ticker_list)):
    try:
        print(a)

        #Cogemos el histórico de precios
        base_url = 'https://fmpcloud.io/api/v3/historical-price-full/'
        ticker_url=ticker_list.iloc[a]
        end_url = '?from=2000-01-01&apikey=27b5adb17295244f3695edd1c6605542'
        ticker_req = base_url + ticker_url+end_url
        historical=(get_jsonparsed_data(ticker_req))
        historical=historical['historical']
        historical_df=pd.DataFrame(historical)
        historical_df['date']=pd.to_datetime(historical_df['date'],errors='coerce')
        
        
        #Le cruzo la tabla calendario para añadir fines de semana
        #Ahora leemos la masterdata de fechas
        date_masterdata=pd.read_sql_table('M_Date',con=engine)
        date_masterdata['date']=date_masterdata['Date']
        historical_df=pd.merge(date_masterdata,historical_df,how='left',on='date')
        historical_df['open'].fillna(method='ffill', inplace=True)
        historical_df['high'].fillna(method='ffill', inplace=True)
        historical_df['low'].fillna(method='ffill', inplace=True)
        historical_df['close'].fillna(method='ffill', inplace=True)
        historical_df['year'] = pd.DatetimeIndex(historical_df['date']).year
        historical_df['month'] = pd.DatetimeIndex(historical_df['date']).month
        historical_df['day'] = pd.DatetimeIndex(historical_df['date']).day
        
        
        
        
        #Vamos a la tabla de earnings
        earnings_df_aux = earnings_df[(earnings_df['symbol'] == ticker_list.iloc[a])]
        earnings_df_aux=pd.to_datetime(earnings_df_aux['date'])
        earnings_df_aux=pd.DataFrame(earnings_df_aux)
        earnings_df_aux['cruce']=earnings_df_aux.index
        earnings_df_aux['year'] = pd.DatetimeIndex(earnings_df_aux['date']).year
        earnings_df_aux['month'] = pd.DatetimeIndex(earnings_df_aux['date']).month
        earnings_df_aux['day'] = pd.DatetimeIndex(earnings_df_aux['date']).day
        
        #Creamos las fechas que serán necesarias para calcular la diferencia precios los dias antes y posteriores
        
        #AÑADIMOS LAS FECHAS PARA TODOS LOS DIAS DE ANALISIS
        earnings_df_aux_30=add_days(earnings_df_aux,-30)
        earnings_df_aux_27=add_days(earnings_df_aux,-27)
        earnings_df_aux_24=add_days(earnings_df_aux,-24)
        earnings_df_aux_21=add_days(earnings_df_aux,-21)
        earnings_df_aux_18=add_days(earnings_df_aux,-18)
        earnings_df_aux_15=add_days(earnings_df_aux,-15)
        earnings_df_aux_12=add_days(earnings_df_aux,-12)
        earnings_df_aux_9=add_days(earnings_df_aux,-9)
        earnings_df_aux_6=add_days(earnings_df_aux,-6)
        earnings_df_aux_3=add_days(earnings_df_aux,-3)
        earnings_df_aux_1=add_days(earnings_df_aux,-1)
        earnings_df_aux_1p=add_days(earnings_df_aux,1)
        earnings_df_aux_3p=add_days(earnings_df_aux,3)
        earnings_df_aux_6p=add_days(earnings_df_aux,6)
        earnings_df_aux_9p=add_days(earnings_df_aux,9)
        earnings_df_aux_12p=add_days(earnings_df_aux,12)
        earnings_df_aux_15p=add_days(earnings_df_aux,15)
        earnings_df_aux_18p=add_days(earnings_df_aux,18)
        earnings_df_aux_21p=add_days(earnings_df_aux,21)
        earnings_df_aux_24p=add_days(earnings_df_aux,24)
        earnings_df_aux_27p=add_days(earnings_df_aux,27)
        earnings_df_aux_30p=add_days(earnings_df_aux,30)
        
         
     
        
        results_df=pd.DataFrame()
        #Cruzamos para quedarnos solo con los dias donde realmente queremos analizar el precio
        earnings_df_aux_price=pd.merge(earnings_df_aux,historical_df,how='left',on=['year','month','day'])
        results_df['date']=earnings_df_aux_price.date_x
        results_df['close_earning_date']=earnings_df_aux_price.close
        
        earnings_df_aux_1_price=pd.merge(earnings_df_aux_1,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_1']=earnings_df_aux_1_price.close
        
        earnings_df_aux_3_price=pd.merge(earnings_df_aux_3,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_3']=earnings_df_aux_3_price.close
        
        earnings_df_aux_6_price=pd.merge(earnings_df_aux_6,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_6']=earnings_df_aux_6_price.close
        
        earnings_df_aux_9_price=pd.merge(earnings_df_aux_9,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_9']=earnings_df_aux_9_price.close
        
        earnings_df_aux_12_price=pd.merge(earnings_df_aux_12,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_12']=earnings_df_aux_12_price.close
        
        
        earnings_df_aux_15_price=pd.merge(earnings_df_aux_15,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_15']=earnings_df_aux_15_price.close
        
        earnings_df_aux_18_price=pd.merge(earnings_df_aux_18,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_18']=earnings_df_aux_18_price.close
        
        earnings_df_aux_21_price=pd.merge(earnings_df_aux_21,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_21']=earnings_df_aux_21_price.close
        
        earnings_df_aux_24_price=pd.merge(earnings_df_aux_24,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_24']=earnings_df_aux_24_price.close
        
        earnings_df_aux_27_price=pd.merge(earnings_df_aux_27,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_27']=earnings_df_aux_27_price.close
        
        
        earnings_df_aux_30_price=pd.merge(earnings_df_aux_30,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_30']=earnings_df_aux_30_price.close
        
        
        
        earnings_df_aux_1_price_p=pd.merge(earnings_df_aux_1p,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_1p']=earnings_df_aux_1_price_p.close
        results_df['open_earning_date_1p']=earnings_df_aux_1_price_p.open
        results_df['high_earning_date_1p']=earnings_df_aux_1_price_p.high
        results_df['low_earning_date_1p']=earnings_df_aux_1_price_p.high
        
        earnings_df_aux_3_price_p=pd.merge(earnings_df_aux_3p,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_3p']=earnings_df_aux_3_price_p.close
        
        earnings_df_aux_6_price_p=pd.merge(earnings_df_aux_6p,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_6p']=earnings_df_aux_6_price_p.close
        
        earnings_df_aux_9_price_p=pd.merge(earnings_df_aux_9p,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_9p']=earnings_df_aux_9_price_p.close
        
        earnings_df_aux_12_price_p=pd.merge(earnings_df_aux_12p,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_12p']=earnings_df_aux_12_price_p.close
        
        earnings_df_aux_15_price_p=pd.merge(earnings_df_aux_15p,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_15p']=earnings_df_aux_15_price_p.close
        
        earnings_df_aux_18_price_p=pd.merge(earnings_df_aux_18p,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_18p']=earnings_df_aux_18_price_p.close
        
        earnings_df_aux_21_price_p=pd.merge(earnings_df_aux_21p,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_21p']=earnings_df_aux_21_price_p.close
        
        earnings_df_aux_24_price_p=pd.merge(earnings_df_aux_24p,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_24p']=earnings_df_aux_24_price_p.close
        
        earnings_df_aux_27_price_p=pd.merge(earnings_df_aux_27p,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_27p']=earnings_df_aux_27_price_p.close
        
        earnings_df_aux_30_price_p=pd.merge(earnings_df_aux_30p,historical_df,how='left',on=['year','month','day'])
        results_df['close_earning_date_30p']=earnings_df_aux_30_price_p.close
        
 
        
 
    
        #Creamos los calculos finales
        results_df['% incr. 27/30']=round((results_df['close_earning_date_27']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 24/30']=round((results_df['close_earning_date_24']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 21/30']=round((results_df['close_earning_date_21']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 18/30']=round((results_df['close_earning_date_18']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 15/30']=round((results_df['close_earning_date_15']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 12/30']=round((results_df['close_earning_date_12']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 9/30']=round((results_df['close_earning_date_9']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 6/30']=round((results_df['close_earning_date_6']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 3/30']=round((results_df['close_earning_date_3']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 1/30']=round((results_df['close_earning_date_1']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 1p/30']=round((results_df['close_earning_date_1p']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 3p/30']=round((results_df['close_earning_date_3p']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 6p/30']=round((results_df['close_earning_date_6p']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 9p/30']=round((results_df['close_earning_date_9p']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 12p/30']=round((results_df['close_earning_date_12p']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 15p/30']=round((results_df['close_earning_date_15p']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 18p/30']=round((results_df['close_earning_date_18p']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 21p/30']=round((results_df['close_earning_date_21p']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 24p/30']=round((results_df['close_earning_date_24p']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 27p/30']=round((results_df['close_earning_date_27p']/results_df['close_earning_date_30'])-1,6)
        results_df['% incr. 30p/30']=round((results_df['close_earning_date_30p']/results_df['close_earning_date_30'])-1,6)
        
        
        results_df['% incr. open aft/close bef earnings']=round((results_df['open_earning_date_1p']/results_df['close_earning_date'])-1,6)
        results_df['% incr. high aft/close bef earnings']=round((results_df['high_earning_date_1p']/results_df['close_earning_date'])-1,6)
        results_df['% incr. low aft/close bef earnings']=round((results_df['low_earning_date_1p']/results_df['close_earning_date'])-1,6)
        results_df['% incr. close aft/close bef earnings']=round((results_df['close_earning_date_1p']/results_df['close_earning_date'])-1,6)
        results_df['% incr. open bef/close bef earnings']=round((results_df['close_earning_date_1p']/results_df['open_earning_date_1p'])-1,6)
        results_df['% incr. close bef/close 15bef earnings']=round((results_df['close_earning_date']/results_df['close_earning_date_15'])-1,6)
        results_df['% incr. close bef/close 3bef earnings']=round((results_df['close_earning_date']/results_df['close_earning_date_3'])-1,6)
        
        results_df['% incr. close aft15/close aft earnings']=round((results_df['close_earning_date_15p']/results_df['close_earning_date_1p'])-1,6)
        results_df['% incr. close aft3/close aft earnings']=round((results_df['close_earning_date_3p']/results_df['close_earning_date_1p'])-1,6)
        results_df['symbol']=ticker_list.iloc[a]
        results_df['Q'] = results_df.date.apply(define_q)
        results_df.drop_duplicates(subset=['symbol','Q'],inplace=True)
        results_df.to_sql('ST_Earning_Analysis2', con = engine, if_exists = 'append',index=False)
        
    except:
        pass
    
















