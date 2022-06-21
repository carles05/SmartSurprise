##############################################################################
from Datamart import DataMart_generator
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import urllib

params = urllib.parse.quote_plus("Driver={ODBC Driver 17 for SQL Server};Server=tcp:finbainservidor.database.windows.net,1433;Database=FinbainBBDD;Uid=finbainbcn;Pwd=Finbain2020BCN;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True)
connection = engine.connect()
##############################################################################


##############################################################################
######  SECTORS DATAMARTS                                               ######
##############################################################################

SP = pd.read_sql_table('M_Symbol_Profile',con=engine)
SP = SP[SP['mktCap']>1e9]
sectors = (SP.groupby('sector')['symbol'].count()>100)
sectors_out = (sectors==False)
sectors = sectors[sectors].index.drop('')
sectors_out = sectors_out[sectors_out].index.append(pd.Index(['']))
sector_symbols = SP[SP['sector'].isin(sectors)].set_index('sector')

dict_symbols = {}

for sector in list(sectors):
    if sector in ['Industrials']: #, 'Real Estate', 'Technology', 'Utilities'
        print('Loading '+sector)
        subset = list(sector_symbols.loc[sector]['symbol'])
        DM = DataMart_generator(subset)
        dict_symbols[sector] = DM.symbols
        DM.upload_DataMart(sector,2)
    
##############################################################################
