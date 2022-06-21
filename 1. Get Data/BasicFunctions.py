#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 18:43:55 2020

@author: carlesferreres
"""
import pandas as pd
import numpy as np
from urllib.request import urlopen
import json
import urllib
import datetime


def dateToQuarter(df):
    try:
        input_date = df.fillingDate
        if input_date != input_date:
            input_date = df.acceptedDate
        input_date = pd.to_datetime(input_date)
        aux = pd.DataFrame({'Q': [pd.to_datetime(str(input_date.year - 1) + '-12-31'),
                                  pd.to_datetime(str(input_date.year) + '-03-31'),
                                  pd.to_datetime(str(input_date.year) + '-06-30'),
                                  pd.to_datetime(str(input_date.year) + '-09-30'),
                                  pd.to_datetime(str(input_date.year) + '-12-31')],
                            'Diff': [input_date - pd.to_datetime(str(input_date.year - 1) + '-12-31'),
                                     input_date - pd.to_datetime(str(input_date.year) + '-03-31'),
                                     input_date - pd.to_datetime(str(input_date.year) + '-06-30'),
                                     input_date - pd.to_datetime(str(input_date.year) + '-09-30'),
                                     input_date - pd.to_datetime(str(input_date.year) + '-12-31')]})
        aux = aux[aux.Diff >= datetime.timedelta(days=0)]
        quarter = aux.Q[aux.Diff == aux.Diff.min()]
        return quarter.iloc[0]
    except:
        try:
            input_date = df.date
            input_date = pd.to_datetime(input_date)
            aux = pd.DataFrame({'Q': [pd.to_datetime(str(input_date.year - 1) + '-12-31'),
                                      pd.to_datetime(str(input_date.year) + '-03-31'),
                                      pd.to_datetime(str(input_date.year) + '-06-30'),
                                      pd.to_datetime(str(input_date.year) + '-09-30'),
                                      pd.to_datetime(str(input_date.year) + '-12-31')],
                                'Diff': [abs(input_date - pd.to_datetime(str(input_date.year - 1) + '-12-31')),
                                         abs(input_date - pd.to_datetime(str(input_date.year) + '-03-31')),
                                         abs(input_date - pd.to_datetime(str(input_date.year) + '-06-30')),
                                         abs(input_date - pd.to_datetime(str(input_date.year) + '-09-30')),
                                         abs(input_date - pd.to_datetime(str(input_date.year) + '-12-31'))]})
            aux = aux[aux.Diff <= datetime.timedelta(days=10)]
            quarter = aux.Q[aux.Diff == aux.Diff.min()]
            return quarter.iloc[0]
        except:
            return np.nan


def readDataURL(url):
    """Takes a url (str). Returns a dataframe """
    response = urlopen(url)
    data = response.read().decode("utf-8")
    data = pd.DataFrame(json.loads(data))
    return data


def createSQLConnection():
    params = urllib.parse.quote_plus("Driver={ODBC Driver 17 for SQL Server};Server=tcp:finbainservidor.database.windows.net,1433;Database=FinbainBBDD;Uid=finbainbcn;Pwd=Finbain2020BCN;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True)
    connection = engine.connect()
    return connection
    
def getTickerList():
    """Returns a list object with symbols where mktCap is bigger than 1e9
    and where sector contains more than 100 companies"""
    
    url = ("https://financialmodelingprep.com/api/v3/stock/list?apikey=1ec220cd25cd422bda50266e163ecfc1")
    data = readDataURL(url)
    end_url = '?apikey=1ec220cd25cd422bda50266e163ecfc1'
    base_url = ' https://fmpcloud.io/api/v3/profile/'
    data['symbol_url'] = base_url+data.symbol+end_url 
    data['symbol_profile'] = data.symbol_url.apply(readDataURL)
    symbol_profile = pd.concat(list(data['symbol_profile']))
    symbols = symbol_profile[['symbol', 'sector', 'industry', 'mktCap']]
    symbols = symbols[(symbols.mktCap > 1e9) & (symbols.sector != '')]
    sectors = symbols.groupby(by=['sector'])['symbol'].count()
    sectors = list(sectors[sectors >= 100].index)
    symbols = symbols[symbols.sector.isin(sectors)]
    return list(symbols.symbol)

