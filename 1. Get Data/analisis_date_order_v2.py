#EARNINGS ANALYSIS

from urllib.request import urlopen
import json
import pandas as pd
import numpy as np

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




#DECLARAMOS TODOS LOS DF INICIALES
#Cogemos una tabla de muestra para ver que fechas tiene
earnings_df=pd.read_sql_table('ST_Earnings',con=engine)
symbol_profile_df=pd.read_sql_table('M_Symbol_Profile',con=engine)
earnings_Analysis_df=pd.read_sql_table('ST_Earning_Analysis',con=engine)
sp_500=pd.read_sql_table('M_SP500_list',con=engine)
sp_500['symbol']=sp_500['Symbol']
earnings_df['date']=pd.to_datetime(earnings_df['date'],errors='coerce')



#We join data to Make the analysis
data=pd.merge(earnings_Analysis_df,symbol_profile_df,how='inner',on='symbol')
data_2=pd.merge(data,earnings_df,how='inner',on=['symbol','date'])
data_2=pd.merge(data_2,sp_500,how='left',on='symbol')

#We drop no necesary columns
data_3=data_2.drop(columns=['close_earning_date','close_earning_date_1','close_earning_date_3','close_earning_date_6','close_earning_date_9',
                            'close_earning_date_12','close_earning_date_15','close_earning_date_18','close_earning_date_21','description'])

data_3['marketcap_cat']= np.where(data_3.mktCap<1000000000,'1.<1B',np.where(data_3.mktCap<10000000000,'2.<10B',np.where(data_3.mktCap<100000000000,'3.<100B',
                         np.where(data_3.mktCap<1000000000000,'4.<1000B',np.where(data_3.mktCap<10000000000000,'5.<10000B','6.>100000B')))))



#CALCULAR SURPRISE RATIO
data_3['surprise_ratio']=np.where((data_3['eps']>=0) & (data_3['epsEstimated']>=0),(data_3['eps']/data_3['epsEstimated'])-1,
np.where((data_3['eps']<0) & (data_3['epsEstimated']>0),-1,
np.where((data_3['eps']>0) & (data_3['epsEstimated']<0), 1,
np.where((data_3['eps']<0) & (data_3['epsEstimated']<0),(data_3['eps']-data_3['epsEstimated'])/abs(data_3['epsEstimated']),0))))







data_3['year'] = pd.DatetimeIndex(data_3['date']).year
data_3['month'] = pd.DatetimeIndex(data_3['date']).month




data_3['surprise_ratio_cat']= np.where(data_3.surprise_ratio<-0.2,'1.<-20%',np.where(data_3.surprise_ratio<-0.1,'2.<-10%',np.where(data_3.surprise_ratio<-0.05,'3.<-5%',
                              np.where(data_3.surprise_ratio<0,'4.<0%',np.where(data_3.surprise_ratio<0.05,'5.<5%',np.where(data_3.surprise_ratio<0.1,'6.<10%',
                              np.where(data_3.surprise_ratio<0.2,'7.<20%','8.>20%')))))))



#PONER EMPRESAS QUE SIEMPRE TENGAN SURPRISE RATIO POSITIVO POR TRAMEADOF

symbol_list=data_3['symbol'].unique()
data_4=pd.DataFrame()
for i in range(0,len(symbol_list)):
    print(i)
    data_aux=data_3[data_3.symbol==symbol_list[i]]
    data_aux_aux=data_aux
    data_aux_aux['flag_surprise_ratio>0']=np.where(data_aux_aux.surprise_ratio>0,1,0)
    data_aux_aux['% surprise_ratio>0']=round(data_aux_aux['flag_surprise_ratio>0'].mean(),3)
    #AHORA LA VARIABLE QUE USAREMOS REALMENTE
    data_aux_aux['Cumple surprise_ratio>0 en +80%veces']=np.where(data_aux_aux['% surprise_ratio>0']>0.8,1,0)
    data_aux_aux['Cumple surprise_ratio<0 en +80%veces']=np.where(data_aux_aux['% surprise_ratio>0']<0.2,1,0)
    
    data_aux_aux['flag_surprise_ratio>5%']=np.where(data_aux_aux.surprise_ratio>0.05,1,0)
    data_aux_aux['% surprise_ratio>5%']=round(data_aux_aux['flag_surprise_ratio>5%'].mean(),3)
    #AHORA LA VARIABLE QUE USAREMOS REALMENTE
    data_aux_aux['Cumple surprise_ratio>5 en +80%veces']=np.where(data_aux_aux['% surprise_ratio>5%']>0.8,1,0)

    data_aux_aux['flag_surprise_ratio<5%']=np.where(data_aux_aux.surprise_ratio<0.05,1,0)
    data_aux_aux['% surprise_ratio<5%']=round(data_aux_aux['flag_surprise_ratio<5%'].mean(),3)
    #AHORA LA VARIABLE QUE USAREMOS REALMENTE
    data_aux_aux['Cumple surprise_ratio<5 en +80%veces']=np.where(data_aux_aux['% surprise_ratio<5%']>0.8,1,0)
    
    #acemos append
    data_4=data_4.append(data_aux_aux)
    
#Un ultimo filtro
data_4['surprise_ratio_outlier_pos']=np.where(data_3.surprise_ratio>5,1,0)
data_4['surprise_ratio_outlier_neg']=np.where(data_3.surprise_ratio<-5,1,0)
    
        
data_4.to_csv('analysis_price_movement_vfin3.csv')
                                                                                                                        
                                                                                                                    
                                                                                                                    



a=pd.read_csv('analysis_price_movement_v1.csv')
b=pd.read_csv('analysis_price_movement_v2.csv')
c=pd.read_csv('analysis_price_movement_v3.csv')
d=pd.read_csv('analysis_price_movement_v4.csv')
e=pd.read_csv('analysis_price_movement_v5.csv')

res=res.append(e)


data_3.to_csv('analysis_price_movement_def2.csv')
