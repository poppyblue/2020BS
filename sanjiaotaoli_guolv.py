import pandas as pd

df = pd.DataFrame(pd.read_csv('sanjiaotaoli.csv'))
# 收益率>0.5%并降序排列
df2 = df.loc[df["Profit(%)"] > 0.5].sort_values(by=['Profit(%)'],ascending=False)
df2.to_csv('./sanjiaotaoli_sort.csv', index=None)
print(df2)
