import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import urllib

class DataMart_generator:
    
    def __init__(self,subset=False):
        
        #DataMart y Target a partir de tablas madre
        self.data_tables=['ST_Balance_Sheet2','ST_Marketcap2','ST_Income_Statement2',
                      'ST_Cash_Flow2','ST_Financial_Ratios2','ST_Financial_Growth2',
                      'ST_Key_Metrcs2','ST_Enterprise_Value2','ST_Earnings2','ST_Earning_Analysis2'] #'ST_Disc_Cash_Flow2' missing
        self.target_variables=['stock_difference','stock_difference_EMA2','earnings','Surprise','Surprise_ratio','% 1p/6a']
        self.tables, self.engine = self.get_data(self.data_tables)
        # self.tables = T
        self.DataMart = pd.DataFrame()
        self.Target = pd.DataFrame()
        self.symbols = {}
        first = True
        
        ##### DATAMART #####
        try:
            print('start datamart')
            for i in range(len(self.data_tables)):
                
                print(self.data_tables[i] + ' is being added to DataMart')
                df = self.tables[self.data_tables[i]].copy()
                df['date'] = pd.to_datetime(df['date'],errors='coerce')
                df.drop(df['date'][df.date.isnull()].index,axis= 0,inplace=True)
                if 'fillingDate' in df.columns:
                    df.rename(columns={'date':'Q'},inplace=True)
                    df['fillingDate'] = pd.to_datetime(df['fillingDate'],errors='coerce')
                    df.drop(df['fillingDate'][df.fillingDate.isnull()].index,axis= 0,inplace=True)
                else:
                    if self.data_tables[i] == 'ST_Earnings2':
                        df.rename(columns={'date':'earnings_date'},inplace=True)
                        df=pd.merge(self.DataMart.reset_index()[['symbol','Q','fillingDate']],df,on=['symbol'])
                        df['diff'] = df['fillingDate']-df['earnings_date']
                        df = df[df.groupby(['symbol','fillingDate'])['diff'].apply(lambda x: x == (x[x>=pd.Timedelta(days= -4)]).min())]
                        df = df[df['diff']<pd.Timedelta(days=90)]
                        df.drop('diff',axis=1,inplace=True)
                    elif self.data_tables[i] == 'ST_Earning_Analysis2':
                        df.rename(columns={'date':'dateX'},inplace=True)
                        df=pd.merge(self.DataMart.reset_index()[['symbol','Q','fillingDate']],df,on=['symbol','Q'])
                    else:
                        df.rename(columns={'date':'Q'},inplace=True)
                        df=pd.merge(self.DataMart.reset_index()[['symbol','Q','fillingDate']],df,on=['symbol','Q'])
                
                ### Data quality step & merge of tables ###
                
                    #symbol column name
                if 'symbol' in df.columns: pass
                elif 'ticker' in  df.columns: df.rename(columns={'ticker':'symbol'},inplace=True)
                if subset:
                    df = df[df['symbol'].isin(subset)]
                df.set_index(['symbol','Q','fillingDate'],inplace=True)
                
                #     #Special tables and dates
                # if self.data_tables[i] == 'ST_Earnings':
                #     df.rename(columns={'date':'earnings_date'},inplace=True)
                #     df=pd.merge(self.DataMart.reset_index('Q')['Q'],df,on=['symbol'])
                #     df['diff'] = df['earnings_date']-df['Q']
                #     df = df[df.groupby(['symbol','Q'])['diff'].apply(lambda x: x == (x[x>pd.Timedelta(days= 0)]).min())]
                #     df = df[df['diff']<pd.Timedelta(days=90)]
                #     df.drop('diff',axis=1,inplace=True)
                #     df.set_index(['symbol','Q','earnings_date'],inplace=True)
                # else:
                #     df.rename(columns={'date':'Q'},inplace=True)
                #     df.set_index(['symbol','Q'],inplace=True)
                    
                    #Formatting cols
                for col in df.columns:
                    df[col] = pd.to_numeric(df[col],errors='coerce')
                    
                    #drop columns because of format
                col_drop = []
                for col in df.columns:
                    if (df.dtypes[col] not in ['float64','int64'] and col != 'earnings_date') or col in ['link','finalLink', 'acceptedDate', 'period','extraction_date']: col_drop.append(col)
                df.drop(col_drop,inplace=True,axis=1)
                print(str(len(col_drop))+' Dropped columns because of format: ' + str(col_drop))
                
                    #duplicates
                df.drop_duplicates(inplace=True)
                
                    #column duplicity
                dup_cols=[]
                col_drop = []
                if not first: dup_cols = self.DataMart.columns.intersection(df.columns)
                for dup in dup_cols:
                    ind = self.DataMart.index.intersection(df.index)
                    if all(self.DataMart.loc[ind][dup].fillna(0)==df.loc[ind][dup].fillna(0)): 
                        col_drop.append(dup)
                print(str(len(col_drop))+' Duplicated columns dropped between '+self.data_tables[i]+' and DataMart: '+ str(col_drop))
                df.drop(col_drop,inplace=True,axis=1)
    
                    #check duplicates
                if any(np.array(df.index.value_counts()>1)):
                    print('Error: Trimestre duplicado con datos distintos en la tabla ' + self.data_tables[i])
                    break
                
                    #Filling DataMart  
                if first:
                    self.DataMart = df
                    first = False
                else:
                    if self.data_tables[i] == 'ST_Earning_Analysis2':
                        df['% 1p/6a'] = round(df['close_earning_date_1p']/df['close_earning_date_6']-1,6)
                        df = df[['symbol','Q','% incr. 6/30','% 1p/6a']]
                        self.DataMart = pd.merge(self.DataMart, df, on=['symbol','Q','fillingDate'],how='left')
                    else:
                        self.DataMart = pd.merge(self.DataMart, df, on=['symbol','Q','fillingDate'],how='left')
                        max_dup = self.DataMart.index.value_counts().max()
                        if max_dup > 1: print('Maximum duplicated indexes: '+ str(max_dup))
                self.DataMart.sort_index(ascending=False,inplace=True)
                
                print('Done')
        except:
            print('DataMart failed')
                
        # try:
            # Check de NaN: drop NaN-abundant columns, backfill occasional NaN and drop unfillable rows still containing NaN
        n_symbols = len(self.DataMart.reset_index()['symbol'].unique())
        overall_cols= pd.DataFrame([0]*len(self.DataMart.columns)).T
        overall_cols.columns= self.DataMart.columns
        for symb in self.DataMart.reset_index()['symbol'].unique():
            self.symbols[symb]={}
            
        j = 0
        Null_matrix = self.DataMart.isnull().groupby('symbol').mean()<0.1
        for k in self.symbols.keys():
            j+=1
            print(str(j/n_symbols*100 )+ ' %')
            self.symbols[k]['cols'] = Null_matrix.loc[k][Null_matrix.loc[k]].index
            for col in self.symbols[k]['cols']:
                overall_cols[col] += 1/n_symbols
                self.DataMart[col][k]=self.DataMart[col][k].fillna(method='bfill')
                
            a = self.DataMart.reset_index()[self.DataMart.reset_index()['symbol']==k].set_index(['symbol','Q','fillingDate'])[self.symbols[k]['cols']].isnull().mean(axis=1)>0
            if j != 1: Null_matrix_row = Null_matrix_row.append(a)
            else: Null_matrix_row = a.copy()
            
        self.symbols['overall'] = {}
        for p_level in np.linspace(10,100,10):
            self.symbols['overall']['cols'+str(p_level)]= overall_cols.loc[0][overall_cols.loc[0]>=(p_level/100)-0.001].index
        nan_rows = Null_matrix_row[Null_matrix_row].index
        self.DataMart.drop(nan_rows,axis=0,inplace=True)
        
    
        # Delete symbols with less than a year of historical quality data
        sym_count = self.DataMart.reset_index(['Q','fillingDate']).index.value_counts()
        self.DataMart.reset_index(['Q','fillingDate'],inplace=True)
        self.DataMart=self.DataMart.drop(sym_count[sym_count<5].index).reset_index().set_index(['symbol','Q','fillingDate'])
        
        # Diccionario de intervalos de tiempo por signo    
        for symb in self.DataMart.reset_index()['symbol'].unique():
            self.symbols[symb]['int'] = [self.DataMart.reset_index('Q')['Q'].loc[symb].min(), self.DataMart.reset_index('Q')['Q'].loc[symb].max()]
            self.symbols[symb]['int_filling'] = [self.DataMart.reset_index('fillingDate')['fillingDate'].loc[symb].min(), self.DataMart.reset_index('fillingDate')['fillingDate'].loc[symb].max()]
            
        # Add custom variables
        new_cols = ['eps_SMA1Y','eps_SMA3Y','eps_EMA1Y','eps_EMA3Y','epsQ_SMA2Y','epsQ_SMA4Y','eps_growthratio_SMA1Y',
                    'eps_growthratio_SMA3Y','eps_growthratio_EMA1Y','eps_growthratio_EMA3Y','Surprise_ratio_SMA1Y',
                    'Surprise_ratio_SMA3Y','Surprise_ratio_EMA1Y','Surprise_ratio_EMA3Y','SurpriseQ_ratio_SMA2Y','SurpriseQ_ratio_SMA4Y']
        self.DataMart.sort_index(ascending=True,inplace=True)
        
        if 'eps' in self.DataMart.columns:
            EPS = 'eps'
        elif 'eps_y' in self.DataMart.columns:
            EPS = 'eps_y'
        else: print('problem with earnings merge')
            
        eps_group =self.DataMart[[EPS]].groupby('symbol')
        self.DataMart['eps_SMA1Y'] = eps_group.rolling(window=4,min_periods=1).mean().reset_index(0,drop=True)
        self.DataMart['eps_SMA3Y'] = eps_group.rolling(window=12,min_periods=1).mean().reset_index(0,drop=True)
        self.DataMart['eps_EMA1Y'] = eps_group.apply(lambda x: x.ewm(span=4,min_periods=1).mean()).reset_index(0,drop=True)
        self.DataMart['eps_EMA3Y'] = eps_group.apply(lambda x: x.ewm(span=12,min_periods=1).mean()).reset_index(0,drop=True)
        self.DataMart['epsQ_SMA2Y'] = eps_group.rolling(window=9,min_periods=1).apply(lambda x: x[::4].mean()).reset_index(0,drop=True)
        self.DataMart['epsQ_SMA4Y'] = eps_group.rolling(window=17,min_periods=1).apply(lambda x: x[::4].mean()).reset_index(0,drop=True)
        
        eps_growth_group = self.DataMart[[EPS]].pct_change().groupby('symbol')
        self.DataMart['eps_growthratio_SMA1Y'] = eps_growth_group.rolling(window=4,min_periods=1).mean().reset_index(0,drop=True)
        self.DataMart['eps_growthratio_SMA3Y'] = eps_growth_group.rolling(window=12,min_periods=1).mean().reset_index(0,drop=True)
        self.DataMart['eps_growthratio_EMA1Y'] = eps_growth_group.apply(lambda x: x.ewm(span=4,min_periods=1).mean()).reset_index(0,drop=True)
        self.DataMart['eps_growthratio_EMA3Y'] = eps_growth_group.apply(lambda x: x.ewm(span=12,min_periods=1).mean()).reset_index(0,drop=True)
        
        surprise_ratio_group = (self.DataMart['Surprise'].copy()/np.abs(self.DataMart['epsEstimated'].copy())).groupby('symbol')
        self.DataMart['Surprise_ratio_SMA1Y'] = surprise_ratio_group.rolling(window=4,min_periods=1).mean().reset_index(0,drop=True)
        self.DataMart['Surprise_ratio_SMA3Y'] = surprise_ratio_group.rolling(window=12,min_periods=1).mean().reset_index(0,drop=True)
        self.DataMart['Surprise_ratio_EMA1Y'] = surprise_ratio_group.apply(lambda x: x.ewm(span=4,min_periods=1).mean()).reset_index(0,drop=True)
        self.DataMart['Surprise_ratio_EMA3Y'] = surprise_ratio_group.apply(lambda x: x.ewm(span=12,min_periods=1).mean()).reset_index(0,drop=True)
        self.DataMart['SurpriseQ_ratio_SMA2Y'] = surprise_ratio_group.rolling(window=9,min_periods=1).apply(lambda x: x[::4].mean()).reset_index(0,drop=True)
        self.DataMart['SurpriseQ_ratio_SMA4Y'] = surprise_ratio_group.rolling(window=17,min_periods=1).apply(lambda x: x[::4].mean()).reset_index(0,drop=True)

        self.DataMart.sort_index(ascending=False,inplace=True)
        
        for symb in self.symbols.keys():
            if symb != 'overall': self.symbols[symb]['cols']=self.symbols[symb]['cols'].append(pd.Index(new_cols))
            else:
                for p_level in np.linspace(10,100,10):
                    self.symbols['overall']['cols'+str(p_level)]=self.symbols['overall']['cols'+str(p_level)].append(pd.Index(new_cols))

            
        # except:
        #     print('Error data process')
        
        
        ##### TARGET #####
        try:
            print('start target')
            self.Target = self.DataMart.reset_index()[['symbol','Q','fillingDate']].set_index(['symbol','Q','fillingDate'])
            
            for t in self.target_variables:
                
                if t == 'stock_difference':
                    print(t + ' in progress')
                    self.Target[t] = np.tanh((np.append(0,self.DataMart['stockPrice'][:-1].values) - self.DataMart['stockPrice'])/(0.1*self.DataMart['stockPrice'].values))
                    for symbol in self.DataMart.reset_index()['symbol'].unique():
                        self.Target[t][(symbol, self.symbols[symbol]['int'][1], self.symbols[symbol]['int_filling'][1])] = np.NAN
                    print(t + ' done')
                
                # elif t == 'stock_difference_EMA2':
                #     if 'stock_difference' not in self.Target.columns: 
                #         print('Error: stock_difference derivated target variables require stock_difference to be calculated.')
                #         break
                #     self.Target[t] = 0
                #     for symbol in self.DataMart.reset_index()['symbol'].unique():
                #         aux = self.Target['stock_difference'][symbol].copy()
                #         aux = aux.ewm(span=2).mean()
                #         self.Target[t][symbol]=aux
                
                elif t == 'earnings':
                    print(t + ' in progress')
                    # self.Target[t] = self.DataMart['eps_y'].copy().shift(1)
                    # self.DataMart.drop(['eps_x','eps_y'],inplace=True,axis=1)
                    # for symb in self.symbols.keys():
                    #     if symb != 'overall': self.symbols[symb]['cols']=self.symbols[symb]['cols'].drop(['eps_x','eps_y'],errors='ignore')
                    #     else:
                    #         for p_level in np.linspace(10,100,10):
                    #             self.symbols['overall']['cols'+str(p_level)]=self.symbols['overall']['cols'+str(p_level)].drop(['eps_x','eps_y'],errors='ignore')
                    self.Target[t] = self.DataMart[EPS].copy().shift(1)
                    if EPS == 'eps_y':
                        self.DataMart.drop(['eps_x','eps_y'],inplace=True,axis=1)
                        for symb in self.symbols.keys():
                            if symb != 'overall': self.symbols[symb]['cols']=self.symbols[symb]['cols'].drop(['eps_x','eps_y'],errors='ignore')
                            else:
                                for p_level in np.linspace(10,100,10):
                                    self.symbols['overall']['cols'+str(p_level)]=self.symbols['overall']['cols'+str(p_level)].drop(['eps_x','eps_y'],errors='ignore')
                    elif EPS == 'eps': 
                        self.DataMart.drop([EPS],inplace=True,axis=1)
                        for symb in self.symbols.keys():
                            if symb != 'overall': self.symbols[symb]['cols']=self.symbols[symb]['cols'].drop([EPS],errors='ignore')
                            else:
                                for p_level in np.linspace(10,100,10):
                                    self.symbols['overall']['cols'+str(p_level)]=self.symbols['overall']['cols'+str(p_level)].drop([EPS],errors='ignore')

                    else: 
                        print('problem with eps merge')

                    for symbol in self.DataMart.reset_index()['symbol'].unique():
                        self.Target[t][(symbol, self.symbols[symbol]['int'][1], self.symbols[symbol]['int_filling'][1])] = np.NAN
                    print(t + ' done')                    
                elif (t == 'Surprise') | (t=='% 1p/6a'):
                    print(t + ' in progress')
                    self.Target[t] = self.DataMart[t].copy().shift(1)
                    self.DataMart.drop([t],inplace=True,axis=1)
                    for symb in self.symbols.keys():
                        if symb != 'overall': self.symbols[symb]['cols']=self.symbols[symb]['cols'].drop([t],errors='ignore')
                        else:
                            for p_level in np.linspace(10,100,10):
                                self.symbols['overall']['cols'+str(p_level)]=self.symbols['overall']['cols'+str(p_level)].drop([t],errors='ignore')
                    for symbol in self.DataMart.reset_index()['symbol'].unique():
                        self.Target[t][(symbol, self.symbols[symbol]['int'][1], self.symbols[symbol]['int_filling'][1])] = np.NAN
                    print(t + ' done')
                    
                elif t == 'Surprise_ratio':
                    print(t + ' in progress')
                    try:
                        self.Target[t] = self.Target['Surprise'].copy()/np.abs(self.DataMart['epsEstimated'].copy().shift(1))
                        print(t + ' done')
                    except ZeroDivisionError:
                        print('Zero division error')

        except:
                print('Error in Target')
        
        
        
    def get_data(self, data_tables=[]):
        params = urllib.parse.quote_plus("Driver={ODBC Driver 17 for SQL Server};Server=tcp:finbainservidor.database.windows.net,1433;Database=FinbainBBDD;Uid=finbainbcn;Pwd=Finbain2020BCN;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True)
        connection = engine.connect()
        tables = {}
        for table in data_tables:
            tables[table] = pd.read_sql_table(table,con=engine)
        return tables, engine
    
    
    
    def symbol_data(self,symbol,target=True):
        if target:
            return self.DataMart.loc[symbol][self.symbols[symbol]['cols']], self.Target.loc[symbol]
        else:
            return self.DataMart.loc[symbol][self.symbols[symbol]['cols']]
    
       
    def upload_DataMart(self,name=False,version=False):
        DM = self.DataMart[self.symbols['overall']['cols80.0']].reset_index()
        T = self.Target.reset_index()
        for i in DM['symbol'].unique():
            if name: 
                DM[DM['symbol']==i].to_sql('Cor_Fundamental'+name+'_Data_v'+str(version), con = self.engine, if_exists = 'append',index=False)
                T[T['symbol']==i].to_sql('Cor_Target'+name+'_Data_v'+str(version), con = self.engine, if_exists = 'append',index=False)
            else:
                DM[DM['symbol']==i].to_sql('Cor_Fundamental_Data_', con = self.engine, if_exists = 'append',index=False)
                T[T['symbol']==i].to_sql('Cor_Target_Data_', con = self.engine, if_exists = 'append',index=False)