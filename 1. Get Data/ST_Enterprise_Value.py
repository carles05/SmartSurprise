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
enterprise_values = pd.read_csv(os.path.join(DATA_PATH, 'enterprise_values.txt'))

# Convertir date a fecha del quarter
enterprise_values.insert(0, 'reportDate', np.nan)
enterprise_values['reportDate'] = enterprise_values.apply(dateToQuarter, axis=1)
enterprise_values = enterprise_values[enterprise_values.reportDate.notnull()].copy()

# Eliminar columnas de fecha inecesarias
enterprise_values.drop(columns=['date'], inplace=True)