# -*- coding: utf-8 -*-
"""
Created on Mon May 25 11:13:38 2020

@author: Carles Ferreres Vivero
"""


import pandas as pd
from BasicFunctions import createSQLConnection, dateToQuarter, readDataURL, \
getTickerList

# Create list with tables info
tables = pd.DataFrame([{'name':'ST_Financial_Growth',
          'base_url':'https://fmpcloud.io/api/v3/financial-growth//'},
          {'name':'ST_Balance_Sheet',
          'base_url':'https://fmpcloud.io/api/v3/balance-sheet-statement/'}])

# GET SYMBOL LIST
symbols = getTickerList()
symbols = pd.DataFrame(symbols, columns = ['symbol'])
symbols = symbols.head(1) # just for testingc

for index, table in tables.iterrows():
    print('Starting with '+table['name'])
    # Crear link para cada symbol
    base_url = table['base_url']
    end_url = '?period=quarter&apikey=27b5adb17295244f3695edd1c6605542'
    symbols['symbol_url'] = base_url+symbols['symbol']+end_url
    
    # Leer datos de cada ticker
    table_name = table['name']
    symbols[table_name] = symbols['symbol_url'].apply(readDataURL)
    
    # Construir un dataframe con todos los resultados
    final_table = pd.concat(list(symbols[table_name]))
    ## Convertir fecha a quarter
    ## Eliminar duplicados de quarters
    
    # Nos Conectamos a la BBDD y escribimos la tabla
    #connection = createSQLConnection()
    #financial_growth_df.to_sql('ST_Financial_Growth_v3', con = engine, if_exists = 'append',index=False)
    final_table.head(1).transpose().to_excel('Column Analysis/'+table_name+'.xlsx')
