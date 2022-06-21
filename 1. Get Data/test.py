import pandas as pd
import os

DATA_PATH = './SavedData'

# CASH FLOW
cash_flow = pd.read_csv(os.path.join(DATA_PATH, 'cash_flow.txt'))


# BALANCE SHEET
balance_sheet = pd.read_csv(os.path.join(DATA_PATH, 'balance_sheet.txt'))
earnings = pd.read_csv(os.path.join(DATA_PATH, 'earnings.txt'))
enterprise_values = pd.read_csv(os.path.join(DATA_PATH, 'enterprise_values.txt'))
financial_growth = pd.read_csv(os.path.join(DATA_PATH, 'financial_growth.txt'))
financial_ratios = pd.read_csv(os.path.join(DATA_PATH, 'financial_ratios.txt'))
income_statement = pd.read_csv(os.path.join(DATA_PATH, 'income_statement.txt'))
key_metrics = pd.read_csv(os.path.join(DATA_PATH, 'key_metrics.txt'))
marketcap = pd.read_csv(os.path.join(DATA_PATH, 'marketcap.txt'))
