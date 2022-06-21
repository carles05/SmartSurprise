#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 11 13:27:13 2020

@author: carlesferreres
"""

import pandas as pd
import numpy as np
import os
from GetData.BasicFunctions import dateToQuarter

# Read saved data
DATA_PATH = './SavedData'
cash_flow = pd.read_csv(os.path.join(DATA_PATH, 'cash_flow.txt'))

# Filtrar empresas que reportan en dolares
cash_flow = cash_flow[cash_flow.reportedCurrency == 'USD']

# Eliminar ultimas columnas con links
cash_flow.drop(columns=['link', 'finalLink'], inplace=True)

# Convertir date a fecha del quarter
cash_flow.insert(0, 'reportDate', np.nan)
cash_flow['reportDate'] = cash_flow.apply(dateToQuarter, axis=1)
cash_flow = cash_flow[cash_flow.reportDate.notnull()]

# Poner el periodo correcto
q_dict = {3: 'Q1', 6: 'Q2', 9: 'Q3', 12: 'Q4'}
cash_flow['period'] = cash_flow.reportDate.dt.month.map(q_dict)

# Eliminar columnas de fecha inecesarias
cash_flow.drop(columns=['date', 'acceptedDate'], inplace=True)
cash_flow.drop_duplicates(subset=['reportDate', 'symbol'], keep='first', inplace=True)

# Rellenar filling date en casos donde es NA y tenemos mas de un registro
fdates = cash_flow[['symbol', 'fillingDate', 'reportDate']].copy()
fdates.dropna(axis=0, inplace=True)
fdates.drop_duplicates(subset=['symbol', 'reportDate'], keep='first', inplace=True)
cash_flow = cash_flow.merge(fdates, on=['symbol', 'reportDate'], how='left', suffixes=['', '_1'])
cash_flow['fillingDate'] = cash_flow['fillingDate_1']
cash_flow.drop(columns='fillingDate_1', inplace=True)
cash_flow.fillingDate.fillna('NoDisp', inplace=True)

# Eliminar duplicados sumando valores de registros
cash_flow = cash_flow.groupby(['reportDate', 'symbol', 'reportedCurrency', 'fillingDate', 'period']).sum().reset_index()

# Escribir datos en otra carpeta
cash_flow.to_csv(r'DataPreprocessing/cash_flow.csv', index=False)
