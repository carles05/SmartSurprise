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
key_metrics = pd.read_csv(os.path.join(DATA_PATH, 'key_metrics.txt'))

# Convertir date a fecha del quarter
key_metrics.insert(0, 'reportDate', np.nan)
key_metrics['reportDate'] = key_metrics.apply(dateToQuarter, axis=1)
key_metrics = key_metrics[key_metrics.reportDate.notnull()].copy()

# Eliminar columnas de fecha inecesarias
key_metrics.drop(columns=['date'], inplace=True)