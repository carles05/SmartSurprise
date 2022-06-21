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
financial_growth = pd.read_csv(os.path.join(DATA_PATH, 'financial_growth.txt'))

# Convertir date a fecha del quarter
financial_growth.insert(0, 'reportDate', np.nan)
financial_growth['reportDate'] = financial_growth.apply(dateToQuarter, axis=1)
financial_growth = financial_growth[financial_growth.reportDate.notnull()].copy()

# Eliminar columnas de fecha inecesarias
financial_growth.drop(columns=['date'], inplace=True)