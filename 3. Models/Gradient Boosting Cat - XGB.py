# -*- coding: utf-8 -*-
"""
Created on Wed Oct  7 17:20:03 2020

@author: Carles Ferreres
"""

# Import libraries
import xgboost as xgb
import pandas as pd
from sklearn.model_selection import KFold
from sklearn.model_selection import GridSearchCV
from sqlalchemy import create_engine
import urllib
import numpy as np
from openpyxl.workbook import Workbook
from pickle import dump

# Define global variables and functions
seed = 12345
to_predict_cont = ['Surprise','earnings','Surprise_ratio']
to_predict_cat = ['flag_surprise5','flag_surprise10','flag_surprise20','flag_surprise-5','flag_surprise-10','flag_surprise-20']   
to_predict_cat_1 = ['flag_surprise20','flag_surprise-20']
to_drop = ['stock_difference']
to_predict=to_predict_cat+to_predict_cont+to_drop

def clean_dataset(df):
    assert isinstance(df, pd.DataFrame), "df needs to be a pd.DataFrame"
    df.dropna(inplace=True)
    indices_to_keep = ~df.isin([np.nan, np.inf, -np.inf]).any(1)
    return df[indices_to_keep].astype(np.float32)

params = urllib.parse.quote_plus("Driver={ODBC Driver 17 for SQL Server};Server=tcp:finbainservidor.database.windows.net,1433;Database=FinbainBBDD;Uid=finbainbcn;Pwd=Finbain2020BCN;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True)
connection = engine.connect()

sectors = ['Healthcare','Industrials','Financial Services','Technology','Consumer Cyclical']
results = pd.DataFrame()
# Main
for sector in sectors:
    print('-- Starting with '+sector)
    fundamental=pd.DataFrame()
    for chunk in pd.read_sql_table('Cor_Fundamental'+sector+'_Data_v1',con=engine,chunksize=1000):
        fundamental=fundamental.append(chunk)
    target=pd.read_sql_table('Cor_Target'+sector+'_Data_v1',con=engine)
    print('--- Finished reading data')
    df = pd.merge(fundamental, target, how ='left', on = ['symbol','Q','fillingDate'])
    
    df['flag_surprise5']=np.where(df.Surprise_ratio> 0.05, 1, 0)
    df['flag_surprise10']=np.where(df.Surprise_ratio> 0.1, 1, 0)
    df['flag_surprise20']=np.where(df.Surprise_ratio> 0.2, 1, 0)
    df['flag_surprise-5']=np.where(df.Surprise_ratio< -0.05, 1, 0)
    df['flag_surprise-10']=np.where(df.Surprise_ratio< -0.1, 1, 0)
    df['flag_surprise-20']=np.where(df.Surprise_ratio< -0.2, 1, 0)
    df=df.drop(columns=['Q','fillingDate','symbol'])
    try:
        df=df.drop(columns=['earnings_date'])
    except:
        pass
    
    for objective in to_predict_cat_1:
        # X and y split
        print('---- Starting with '+objective)
        df=clean_dataset(df)
        X=df.drop(columns=to_predict)
        y=df[objective]
        # Build Model
        kfold = KFold(n_splits=5, random_state=seed, shuffle=True)
        model = xgb.XGBClassifier()
        param_grid = {"n_estimators" : [10,25,50,75], "max_depth" : [1,2,3,4]}
        # eta: learning rate - default 0.3, lambda: L2 regularization, default 1, alpha: L1 regularization, default 0
        grid = GridSearchCV(estimator=model, param_grid=param_grid, scoring='accuracy', cv=kfold, n_jobs=-1)
        # Train Model
        print('---- Training model')
        grid_results = grid.fit(X,y)
        best_model = grid.best_estimator_
        print(f'Grid Best Score {grid_results.best_score_:.7f}\n\
        Number of Trees {grid_results.best_params_["n_estimators"]:3d}\n\
        Max Depth of Decision Trees {grid_results.best_params_["max_depth"]:3d}')
        filename = 'model_'+sector+'_'+objective
        results = results.append({'Model':filename,'Score':grid_results.best_score_,\
                                  'Best_params': grid_results.best_params_}, ignore_index=True)
        dump(best_model, open(filename, "wb"))

results.to_excel('results.xlsx')
