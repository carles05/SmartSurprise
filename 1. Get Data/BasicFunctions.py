#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 18:43:55 2020

@author: carlesferreres
"""
import pandas as pd
from urllib.request import urlopen
from sqlalchemy import create_engine
import json
import urllib

def dateToQuarter(date):
    if date.month<4:
        return pd.to_datetime(str(date.year-1)+'-12-31')
    elif date.month<7:
        return pd.to_datetime(str(date.year)+'-03-31')
    elif date.month<10:
        return pd.to_datetime(str(date.year)+'-06-30')
    else:
        return pd.to_datetime(str(date.year)+'-09-30')
    
def readDataURL(url):
    """Takes a url (str). Returns a dictionary """
    response = urlopen(url)
    data = response.read().decode("utf-8")
    data = pd.DataFrame(json.loads(data))
    return data

def createSQLConnection():
    params = urllib.parse.quote_plus("Driver={ODBC Driver 17 for SQL Server};Server=tcp:finbainservidor.database.windows.net,1433;Database=FinbainBBDD;Uid=finbainbcn;Pwd=Finbain2020BCN;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True)
    connection = engine.connect()
    return connection
    
