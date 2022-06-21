#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 11 13:27:13 2020

@author: carlesferreres
"""

import pandas as pd
import numpy as np
import os

# Read saved data
DATA_PATH = './SavedData'
marketcap = pd.read_csv(os.path.join(DATA_PATH, 'marketcap.txt'))

# SEEMS WRONG VALUES
