# -*- coding: utf-8 -*-
"""
Created on Mon May 25 11:13:38 2020

@author: Carles Ferreres Vivero
"""


import pandas as pd
from BasicFunctions import createSQLConnection, dateToQuarter, readDataURL, \
getTickerList

# Create list with tables info
tables = pd.DataFrame([{'name': 'ST_Financial_Growth', 'base_url': 'https://fmpcloud.io/api/v3/financial-growth//'},
                       {'name': 'ST_Balance_Sheet', 'base_url': 'https://fmpcloud.io/api/v3/balance-sheet-statement/'},
                       {'name': 'ST_Cash_Flow', 'base_url': 'https://fmpcloud.io/api/v3/cash-flow-statement/'},
                       {'name': 'ST_Enterprise_Value', 'base_url': 'https://fmpcloud.io/api/v3/enterprise-values/'},
                       {'name': 'ST_Financial_Ratios', 'base_url': 'https://fmpcloud.io/api/v3/ratios/'},
                       {'name': 'ST_Income_Statement', 'base_url': 'https://fmpcloud.io/api/v3/income-statement/'},
                       {'name': 'ST_Marketcap', 'base_url': 'https://fmpcloud.io/api/v3/historical-market-capitalization/'},
                       {'name': 'ST_Key_Metrics', 'base_url': 'https://fmpcloud.io/api/v3/key-metrics/'}])

# GET SYMBOL LIST
symbols = getTickerList()
#symbols = pd.DataFrame(symbols, columns = ['symbol'])

for index, table in tables.iterrows():
    print('Starting with '+table['name'])
    # Crear link para cada symbol
    base_url = table['base_url']
    end_url = '?period=quarter&apikey=1ec220cd25cd422bda50266e163ecfc1'
    symbols['symbol_url'] = base_url+symbols['symbol']+end_url
    
    # Leer datos de cada ticker
    table_name = table['name']
    symbols[table_name] = symbols['symbol_url'].apply(readDataURL)
    
    # Construir un dataframe con todos los resultados
    final_table = pd.concat(list(symbols[table_name]))
    
    # Guardar resultados
    final_table.head(1).transpose().to_excel('InputData/'+table_name+'.xlsx')
