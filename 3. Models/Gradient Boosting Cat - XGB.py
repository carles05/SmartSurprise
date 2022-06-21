# -*- coding: utf-8 -*-
"""
Created on Wed Oct  7 17:20:03 2020

@author: Carles Ferreres
"""

# Import libraries
import xgboost as xgb
import pandas as pd
from sklearn.model_selection import StratifiedKFold
from sklearn.model_selection import GridSearchCV
from sqlalchemy import create_engine
import urllib
import numpy as np
from openpyxl.workbook import Workbook
from pickle import dump
from sklearn.model_selection import train_test_split
from yellowbrick.classifier import ROCAUC
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score

def read_data_sector(sector):
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
    df=clean_dataset(df)
    return df

def train_XGB(X_train,y_train):
    # StratifiedKFols for unbalanced classes
    kfold = StratifiedKFold(n_splits=5, random_state=seed, shuffle=True)
    xgbmodel = xgb.XGBClassifier()
    param_grid = {"n_estimators" : [25,50,75,100], "max_depth" : [1,2,3],
                  'objective':['binary:logistic'], 'learning_rate' : [0.1,0.2,0.3]}
    grid = GridSearchCV(estimator=xgbmodel, param_grid=param_grid, scoring='roc_auc', cv=kfold, n_jobs=-1,
                        return_train_score = True)
    # Train Model
    print('---- Training model')
    grid_results = grid.fit(X_train,y_train)
    best_model = grid.best_estimator_
    return best_model, grid_results
        
def test_score_XGB(model, X_test, y_test):
    y_pred = model.predict(X_test)
    score = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    return score, recall

def clean_dataset(df):
    assert isinstance(df, pd.DataFrame), "df needs to be a pd.DataFrame"
    df.dropna(inplace=True)
    indices_to_keep = ~df.isin([np.nan, np.inf, -np.inf]).any(1)
    return df[indices_to_keep].astype(np.float32)


# Define global variables and functions
seed = 12345
to_predict_cont = ['Surprise','earnings','Surprise_ratio']
to_predict_cat = ['flag_surprise5','flag_surprise10','flag_surprise20','flag_surprise-5','flag_surprise-10','flag_surprise-20']
to_predict_cat_1 = ['flag_surprise20']
to_drop = ['stock_difference']
to_predict=to_predict_cat+to_predict_cont+to_drop

params = urllib.parse.quote_plus("Driver={ODBC Driver 17 for SQL Server};Server=tcp:finbainservidor.database.windows.net,1433;Database=FinbainBBDD;Uid=finbainbcn;Pwd=Finbain2020BCN;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True)
connection = engine.connect()

#sectors = ['Healthcare','Industrials','Financial Services','Technology','Consumer Cyclical']
sectors = ['Industrials']
results = pd.DataFrame()
# Main
for sector in sectors:
    sector_df = read_data_sector(sector)
    for objective in to_predict_cat:
        print('--- Starting with '+objective)
        X = sector_df.drop(columns = to_predict)
        y = sector_df[objective]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2,
                                                            random_state=seed, shuffle=True,
                                                            stratify = y)
        model, grid_results = train_XGB(X_train,y_train)
        precision, recall = test_score_XGB(model, X_test, y_test)
        print(f'Grid Best Score {grid_results.best_score_:.7f}\n\
        Number of Trees {grid_results.best_params_["n_estimators"]:3d}\n\
        Max Depth of Decision Trees {grid_results.best_params_["max_depth"]:3d}\n\
        Learning Rate {grid_results.best_params_["learning_rate"]:.2f}\n\
        Precision Test {precision :.5f}\n\
        Recall test {recall:.3f}')
        filename = 'model_'+sector+'_'+objective
        features = sorted(zip(model.feature_importances_, X_train.columns), reverse=True)[:10]
        results = results.append({'Model':filename,'Precision Test':precision,\
                                  'Recall Test':recall,'Best_params': grid_results.best_params_,\
                                 'Features':features}, ignore_index=True)
        dump(model, open(filename, "wb"))

results.to_excel(r'C:\Users\viveroc\Documents\SmartSurprise\SmartSurpriseRepo\results.xlsx')
