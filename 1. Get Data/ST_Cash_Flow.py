#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 11 13:27:13 2020

@author: carlesferreres
"""

import pandas as pd
from BasicFunctions import createSQLConnection, dateToQuarter, readDataURL, \
getTickerList

# Declare table info
table_name = 'ST_Cash_Flow'
base_url = 'https://fmpcloud.io/api/v3/cash-flow-statement/'
end_url = '?period=quarter&apikey=27b5adb17295244f3695edd1c6605542'

# GET SYMBOL LIST
# Try pkl else gettickerlist
try:
    symbols = pd.read_pickle('symbols.pkl')
except:
    symbols = getTickerList()
    symbols = pd.DataFrame(symbols, columns = ['symbol'])
    symbols.to_pickle('symbols.pkl')
    
# Crear link para cada symbol
symbols['symbol_url'] = base_url+symbols['symbol']+end_url

# Leer datos de cada ticker
symbols[table_name] = symbols['symbol_url'].apply(readDataURL)

# Construir un dataframe con todos los resultados
final_table = pd.concat(list(symbols[table_name]))
final_table.to_csv('~/Desktop/Carles/SmartSurprise/SavedData/cash_flow.txt', index=False)
## Convertir fecha a quarter
## Eliminar duplicados de quarters

# Nos Conectamos a la BBDD y escribimos la tabla
#connection = createSQLConnection()
#financial_growth_df.to_sql('ST_Financial_Growth_v3', con = engine, if_exists = 'append',index=False)
