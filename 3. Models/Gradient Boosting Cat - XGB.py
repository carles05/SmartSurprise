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

# Define global variables
seed = 12345

# Read Data from Azure

# Split X and Y

# Build Model
kfold = KFold(n_splits=10, random_state=seed, shuffle=True)
mdel = xbg.XGBClassifier()
param_grid = {"n_etimators" : [50, 100, 150, 200, 250, 300], "max_depth" : [2,3,4,5,6,7,8,9,10]}
    # eta: learning rate - default 0.3, lambda: L2 regularization, default 1, alpha: L1 regularization, default 0
grid = GridSearchCV(estimator=model, param_grid=param_grid, scoring='accuracy', cv=kfold, n_jobs=-1)
# Train Model

grid_results = grid.fit(X,y)


#Earning Model
##############################################################################
######  0) LIBRARIES                                                       ######
##############################################################################

#from scipy.spatial import distance
#from scipy.cluster import hierarchy
SEED=12345
#from sklearn.preprocessing import MinMaxScaler
def clean_dataset(df):
    assert isinstance(df, pd.DataFrame), "df needs to be a pd.DataFrame"
    df.dropna(inplace=True)
    indices_to_keep = ~df.isin([np.nan, np.inf, -np.inf]).any(1)
    return df[indices_to_keep].astype(np.float32)


parameters = pd.DataFrame()


##############################################################################
######  1) PROBLEM SETTLING                                             ######
##############################################################################

params = urllib.parse.quote_plus("Driver={ODBC Driver 17 for SQL Server};Server=tcp:finbainservidor.database.windows.net,1433;Database=FinbainBBDD;Uid=finbainbcn;Pwd=Finbain2020BCN;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True)
connection = engine.connect()

SP = pd.read_sql_table('M_Symbol_Profile',con=engine)
SP = SP[SP['mktCap']>1e9]
sectors = (SP.groupby('sector')['symbol'].count()>100)
sectors_out = (sectors==False)
sectors = list(sectors[sectors].index.drop(''))
sectors = ['Healthcare','Industrials','Financial Services','Technology','Consumer Cyclical']



for sector in sectors:
    print('Starting with '+sector)
    fundamental=pd.DataFrame()
    for chunk in pd.read_sql_table('Cor_Fundamental'+sector+'_Data_v1',con=engine,chunksize=1000):
        fundamental=fundamental.append(chunk)
    target=pd.read_sql_table('Cor_Target'+sector+'_Data_v1',con=engine)

    df = pd.merge(fundamental, target, how ='left', on = ['symbol','Q','fillingDate'])
    #df.drop(['earnings_date'],axis = 1, inplace = True)
    pre_n_rows = len(df)
    df['ind']=df.index
    n_rows = len(df)
    print('Kept rows because of NAN values: '+ str(n_rows) + '/'+str(pre_n_rows))

    ##########################################################################
    ##  2) VARIABLES TO PREDICT                                         ######
    ##########################################################################
    
    
    #Categorical objetive variables to predict
    to_predict_cont = ['Surprise','earnings','Surprise_ratio']
    
    to_predict_cat = ['flag_surprise5','flag_surprise10','flag_surprise20','flag_surprise-5','flag_surprise-10','flag_surprise-20']
    
    to_drop = ['stock_difference']
    
    to_predict=to_predict_cat+to_predict_cont+to_drop #VARIABLES WE CANNOT USE IN THE MODEL

    
    
    
    df['flag_surprise5']=np.where(df.Surprise_ratio> 0.05, 1, 0)
    df['flag_surprise10']=np.where(df.Surprise_ratio> 0.1, 1, 0)
    df['flag_surprise20']=np.where(df.Surprise_ratio> 0.2, 1, 0)
    df['flag_surprise-5']=np.where(df.Surprise_ratio< -0.05, 1, 0)
    df['flag_surprise-10']=np.where(df.Surprise_ratio< -0.1, 1, 0)
    df['flag_surprise-20']=np.where(df.Surprise_ratio< -0.2, 1, 0)
    
    "en el caso que exista la variable earnings date, la borramos"    
    try:
        df=df.drop(columns=['earnings_date'])
    except:
        pass
        

    #Para cada una de las variables explicativas realizaremos feature selection + modelo
    for i in range(len(to_predict_cat)):
        vo=to_predict_cat[i]#variable objetivo para usar en el modelo  
        df = df[df[vo].notnull()] #we drop rows with vo null
        ##########################################################################
        ##  3) Feature Selection                                            ######
        ##########################################################################
        
        print('Starting correlation analysis for '+ sector)
        correlations = df.corr()
        abs_correlations=abs(correlations)
        var = np.abs(correlations[[vo]].drop(to_predict,axis=0)).nlargest(40,[vo])
        model_var = list(var[var > 0.05].index)
        #We save model vars
        filename='vars'+to_predict_cat[i] + sector+'.sav'
        pickle.dump(model_var, open(filename, 'wb'))


    ##########################################################################
    ##  4) Model Training and Test Regresion                            ######
    ##########################################################################
        
        df2=df.drop(columns=['Q','fillingDate','symbol'])
        df2=clean_dataset(df2)
        y=df2[vo]
        X=df2[model_var]
        #train test split
        Xtrain, Xtest, yc_train, yc_test=train_test_split(X,
                                                         y,
                                                         test_size=0.3,
                                                         random_state=SEED)
        #We need to store the index values of test set
        dict_ind_test=pd.DataFrame()
        dict_ind_test['ind']=Xtest.index
        

        
        #We make the model and tune parameters
        lr_list = [0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1]
        depth_list = [2,6,10]
        features_list= [2,6,10]


        accuracy_def=0
        for l in range(len(lr_list)):
            for j in range(len(depth_list)):
                for k in range(len(features_list)):
                    
                    gb_clf = GradientBoostingClassifier(n_estimators=20, learning_rate=lr_list[l], max_features=features_list[k],
                                                       max_depth=depth_list[j], random_state=0)
                    gb_clf.fit(Xtrain, yc_train)
                    predictions = gb_clf.predict(Xtrain)
                    predictions2 = gb_clf.predict(Xtest)
                    print("Accuracy score (training): {0:.3f}".format(gb_clf.score(Xtrain, yc_train)))
                    print("Accuracy score (validation): {0:.3f}".format(gb_clf.score(Xtest, yc_test)))
                    accuracy_test=gb_clf.score(Xtest, yc_test)
                    
                    if accuracy_test>accuracy_def:                                
                        xgb_def=gb_clf
                        accuracy_def=accuracy_test
                        depth=depth_list[j]
                        lr=lr_list[l]
                        features=features_list[k]
            
            
            
            
    ##########################################################################
    ##  4) Model Training and Test Clasification                        ######
    ##########################################################################
                        
    

    ##########################################################################
    ##  5) Predictions                                      ######
    ##########################################################################
                        
        #We get predictions and add necessary variables to get the results                        
        PREDICTION=xgb_def.predict(Xtest)
        PREDICTION=pd.DataFrame(PREDICTION)
        PREDICTION['pred']=PREDICTION.iloc[:,0]
        PREDICTION=PREDICTION.pred
        y=pd.DataFrame(yc_test)
        y['symbol']=df[df['ind'].isin(dict_ind_test.ind)]['symbol']
        y['Q']=df[df['ind'].isin(dict_ind_test.ind)]['Q']
        y['epsReal']=df[df['ind'].isin(dict_ind_test.ind)]['earnings']
        y['Surprise_ratio']=df[df['ind'].isin(dict_ind_test.ind)]['Surprise_ratio']
        y['Surprise']=df[df['ind'].isin(dict_ind_test.ind)]['Surprise']

        y.reset_index(drop=True, inplace=True)
        result_df=pd.concat([y,PREDICTION],axis=1)
        result_df['epsEstimated']=result_df['epsReal']-result_df['Surprise']
        result_df['sector']=sector

        
        
        #Params results and model save
            
        
        par2=(depth,lr,features,accuracy_def)
        par2=pd.DataFrame(par2)
        par2=par2.transpose()
        par2.columns = ["depth", "lr", "features","accuracy_def"]
        par2['model']=to_predict_cat[i]
        par2['sector']=sector
        parameters=parameters.append(par2)

        
        
        result_df.to_excel(r'C:/Users/Adrià/Desktop/Smart_Surprise_Models/'+to_predict_cat[i]+sector+'.xlsx')
        
        
    ##########################################################################
    ##  6) Plot Feature Importance                                      ######
    ##########################################################################
        
        feature_importance = xgb_def.feature_importances_
        sorted_idx = np.argsort(feature_importance)
        pos = np.arange(sorted_idx.shape[0]) + .5
        fig = plt.figure(figsize=(12, 6))
        plt.subplot(1, 2, 1)
        plt.barh(pos, feature_importance[sorted_idx], align='center')
        plt.yticks(pos, np.array(X.columns)[sorted_idx])
        plt.title('Feature Importance '+ sector + ' ' + to_predict_cat[i])
        
        result = permutation_importance(xgb_def, Xtest, yc_test, n_repeats=10,
                                        random_state=42, n_jobs=2)
        sorted_idx = result.importances_mean.argsort()
        plt.subplot(1, 2, 2)
        plt.boxplot(result.importances[sorted_idx].T,
                    vert=False, labels=np.array(X.columns)[sorted_idx])
        plt.title("Permutation Importance (test set)")
        fig.tight_layout()
        plt.show()
        fig.savefig(r'C:/Users/Adrià/Desktop/Smart_Surprise_Models/'+ to_predict_cat[i] + sector +'feat_importance.png')
        
        
    ##########################################################################
    ##  7) Saving Model                                                 ######
    ##########################################################################
        filename=to_predict_cat[i] + sector+'.sav'
        pickle.dump(xgb_def, open(filename, 'wb'))
        
        
        


        
parameters.to_excel(r'C:/Users/Adrià/Desktop/Smart_Surprise_Models/parameters_cat.xlsx')




