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
income_statement = pd.read_csv(os.path.join(DATA_PATH, 'income_statement.txt'))

# Filtrar empresas que reportan en dolares
income_statement = income_statement[income_statement.reportedCurrency == 'USD']

# Eliminar ultimas columnas con links
income_statement.drop(columns=['link', 'finalLink'], inplace=True)

# Convertir date a fecha del quarter
income_statement.insert(0, 'reportDate', np.nan)
income_statement['reportDate'] = income_statement.apply(dateToQuarter, axis=1)
income_statement = income_statement[income_statement.reportDate.notnull()]

# Eliminar columnas de fecha inecesarias
income_statement.drop(columns=['date', 'fillingDate', 'acceptedDate', 'period', 'reportedCurrency'], inplace=True)