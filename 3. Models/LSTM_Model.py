# AUTHOR: EDUARD BADIA
# LAST UPDATE: 08/07/2020


import pandas as pd
import os
import random
import numpy as np
import time
from keras.layers import Dense, Lambda, Layer, Input, Flatten, LSTM
from keras.models import Sequential, Model
from keras.optimizers import Adam
from keras.models import load_model
from keras.callbacks import ModelCheckpoint
from keras import backend as K
from keras.losses import BinaryCrossentropy
from keras.losses import MSE



class LSTM_predictor():
    
    
    def __init__(self, DM, Target, var_size, pred_size, method='LSTM_cont', activation = 'tanh',
                 lr = 0.25, layers = 3, neurons_per_layer = 32, train_test = 0.7, train_symbols=[], test_symbols=[]):
        self.DM = DM.sort_values(['symbol','Q','fillingDate'], ascending=[1,1,1])
        self.Target = Target.sort_values(['symbol','Q','fillingDate'], ascending=[1,1,1])
        self.pred_var_names =['earnings','Surprise','Surprise_ratio']
        print(self.pred_var_names)
        self.method = method
        self.var_size = var_size
        self.pred_size = pred_size
        self.activation = activation
        self.layers = layers
        self.neurons_per_layer = neurons_per_layer
        self.train_test = train_test
        self.learning_rate = lr
        
        self.brain = self.build_brain(self.method)
        
        self.train_symbols = []
        self.test_symbols = []
        symb = self.DM.symbol.unique()
        if train_test:
            for s in symb:
                if np.random.rand() < self.train_test: self.train_symbols.append(s)
                else: self.test_symbols.append(s)
        else:
            if test_symbols == [] or train_symbols == []: print('ERROR: if train_test==False, you need to inform test_symbols and train_symbols')
            self.train_symbols = train_symbols
            self.test_symbols = test_symbols
        self.TRAIN = self.DM[self.DM['symbol'].isin(self.train_symbols)]
        self.TEST = self.DM[self.DM['symbol'].isin(self.test_symbols)]
        self.TRAIN_Target = self.Target[self.Target['symbol'].isin(self.train_symbols)]
        self.TEST_Target = self.Target[self.Target['symbol'].isin(self.test_symbols)]
        
        
    def build_brain(self,method):
        
        if method == 'LSTM_cont':
            brain = Sequential()
            brain.add(LSTM(self.neurons_per_layer, batch_input_shape=(1,1,self.var_size), return_sequences = True, stateful=True))
            b = True
            for i in range(self.layers-1):
                if i == self.layers-2: b = False
                brain.add(LSTM(self.neurons_per_layer, activation = self.activation, return_sequences = b, stateful=True))
            brain.add(Dense(self.pred_size, activation='linear'))
            brain.compile(loss='mse', optimizer=Adam(lr=self.learning_rate))
            
        if method == 'LSTM_cat':
            brain = Sequential()
            brain.add(LSTM(self.neurons_per_layer, batch_input_shape=(1,1,self.var_size), stateful=True))
            for i in range(self.layers-1):
                if i == self.layers-2: b = False
                brain.add(LSTM(self.neurons_per_layer, activation = self.activation, return_sequences = b, stateful = True))
            brain.add(Dense(self.pred_size, activation='linear'))
            brain.compile(loss=BinaryCrossentropy(), optimizer=Adam(lr=self.learning_rate))
            
        if method == 'DNN':
            brain = Sequential()
            brain.add(Dense(self.neurons_per_layer, batch_input_shape=(1,1,self.var_size), stateful=True))
            for i in range(self.layers-1):
                brain.add(Dense(self.neurons_per_layer, activation = self.activation))
            brain.add(Dense(self.pred_size, activation='linear'))
            brain.compile(loss='mse', optimizer=Adam(lr=self.learning_rate))
            
        return brain
            
        
    def train(self, epochs):
        loss = []
        test_loss = []
        test_pred = []
        for e in range(epochs):
            print('Epoch '+str(e+1)+'/'+str(epochs))
            
            e_loss = []
            for s in self.train_symbols:
                TRAIN_SYMB = (self.TRAIN[self.TRAIN['symbol']==s]).drop(['symbol','Q','fillingDate'],axis=1).values
                print(TRAIN_SYMB)
                TRAIN_SYMB_T = (self.TRAIN_Target[self.TRAIN_Target['symbol']==s]).drop(['symbol','Q','fillingDate'],axis=1).values
                print(TRAIN_SYMB_T)
                for i in range(len(TRAIN_SYMB)):
                    L = self.brain.fit(TRAIN_SYMB[i,:].reshape(1,1,self.var_size),TRAIN_SYMB_T[i,:], epochs = 1, batch_size = 1, verbose = 0, shuffle=False)
                    e_loss.append(L.history['loss'][0])
                self.brain.reset_states()
            loss.append(np.array(e_loss).mean())
                
            e_test_loss = []
            for s in self.test_symbols:
                TEST_SYMB = (self.TEST[self.TEST['symbol']==s]).drop(['symbol','Q','fillingDate'],axis=1).values
                TEST_SYMB_T = (self.TEST_Target[self.TEST_Target['symbol']==s]).drop(['symbol','Q','fillingDate'],axis=1).values
                for i in range(len(TEST_SYMB)):
                    TEST_Target_pred = self.brain.predict(TEST_SYMB[i,:].reshape(1,1,self.var_size))
                    test_pred.append(TEST_Target_pred)
                    e_test_loss.append(np.abs(TEST_SYMB_T[i,:]-TEST_Target_pred))
                self.brain.reset_states()
            test_loss.append(np.array(e_test_loss).mean())
            
            print('==> Loss = ' + str(loss[-1]) + ' // Test_Loss = ' + str(test_loss[-1]))
            
        return loss, test_loss, test_pred
    
    def evaluate(self, EDM):
        EDM.sort_values(['symbol','Q','fillingDate'], ascending=[1,1,1], inplace=True)
        evaluate_pred = []
        for s in list(EDM['symbol'].unique()):
            EVALUATE_SYMB = (EDM[EDM['symbol']==s]).drop(['symbol','Q','fillingDate'],axis=1).values
            for i in range(len(EVALUATE_SYMB)):
                EVALUATE_Target_pred = self.brain.predict(EVALUATE_SYMB[i,:].reshape(1,1,self.var_size))
                evaluate_pred.append(EVALUATE_Target_pred)
            self.brain.reset_states()
        evaluate_pred_ = np.array(evaluate_pred)
        PREDICTION = EDM[['symbol','Q','fillingDate']].copy()
        for k in range(len(self.pred_var_names)):
            PREDICTION[self.pred_var_names[k]+'_pred'] = evaluate_pred_[:,k]
        return PREDICTION
    
    def save(self, name):
        self.brain.save(r'./Saved Models/'+name+'.h5')