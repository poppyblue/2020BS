import pandas as pd


df = pd.read_csv('shuanjunxian.csv')
len = len(df)
# print(len)
profit = (((df.portfolio_value[len-1]) - 1000) / (len * 1000)) * 100
print(profit, '%')
