# -*- coding: utf-8 -*-
"""
Created on Mon May 25 11:13:38 2020

@author: Carles Ferreres Vivero
"""


import pandas as pd
from BasicFunctions import createSQLConnection, dateToQuarter, readDataURL


# GET SYMBOL LIST
url = ("https://financialmodelingprep.com/api/v3/stock/list?apikey=b05bd33f2e28209bc4770d5ad2d20ace")
symbols = readDataURL(url)

# Filtramos los tickers con marketcap mayor que X
symbols = symbols[['symbol']]
## Limpiar tickers con marketcap less than 10e9

# Crear link para cada symbol
base_url = 'https://fmpcloud.io/api/v3/financial-growth//'
end_url = '?period=quarter&apikey=27b5adb17295244f3695edd1c6605542'
symbols['symbol_url'] = base_url+symbols['symbol']+end_url

# Leer datos de cada ticker
symbols = symbols.head(10) #Test with 10 records
symbols['fin_growth'] = symbols['symbol_url'].apply(readDataURL)

# Construir un dataframe con todos los resultados
financial_growth = pd.concat(list(symbols['fin_growth']))
## Convertir fecha a quarter
## Eliminar duplicados

# Nos Conectamos a la BBDD y escribimos la tabla
connection = createSQLConnection()
financial_growth_df.to_sql('ST_Financial_Growth_v3', con = engine, if_exists = 'append',index=False)