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
financial_ratios = pd.read_csv(os.path.join(DATA_PATH, 'financial_ratios.txt'))

# Convertir date a fecha del quarter
financial_ratios.insert(0, 'reportDate', np.nan)
financial_ratios['reportDate'] = financial_ratios.apply(dateToQuarter, axis=1)
financial_ratios = financial_ratios[financial_ratios.reportDate.notnull()]

# Eliminar columnas de fecha inecesarias
financial_ratios.drop(columns=['date'], inplace=True)