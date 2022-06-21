#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 11 13:27:13 2020

@author: carlesferreres
"""

import pandas as pd
from BasicFunctions import createSQLConnection, dateToQuarter, readDataURL, getTickerList

import pandas as pd
import numpy as np
import os
from GetData.BasicFunctions import dateToQuarter

# Read saved data
DATA_PATH = './SavedData'
balance_sheet = pd.read_csv(os.path.join(DATA_PATH, 'balance_sheet.txt'))

# Filtrar empresas que reportan en dolares
balance_sheet = balance_sheet[balance_sheet.reportedCurrency == 'USD']

# Eliminar ultimas columnas con links
balance_sheet.drop(columns=['link', 'finalLink'], inplace=True)

# Convertir date a fecha del quarter
balance_sheet.insert(0, 'reportDate', np.nan)
balance_sheet['reportDate'] = balance_sheet.apply(dateToQuarter, axis=1)
balance_sheet = balance_sheet[balance_sheet.reportDate.notnull()]

# Eliminar columnas de fecha inecesarias
balance_sheet.drop(columns=['date', 'fillingDate', 'acceptedDate', 'reportedCurrency'], inplace=True)