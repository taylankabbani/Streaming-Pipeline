import pandas as pd
p1 = "/home/taylan/Documents/FactoryPal/database/workorder"
p2 = "/home/taylan/Documents/FactoryPal/database/metrics"

df1 = pd.read_parquet(p1, engine='fastparquet')
df2 = pd.read_parquet(p2, engine='fastparquet')

df1_fil = df1.loc[df1["product"] == 1]
df2_fil = df2[df2["id"] == 1]
print(df1_fil)
# print(df2_fil)

s = df1_fil['produced'] = df1_fil['production'].shift(
    -1) - df1_fil['production']

print(s)
# merged_df = pd.merge(df1_fil, df2_fil, on='time', how='inner')
# print(merged_df)
# df = pd.read_json(
#     "/home/taylan/Documents/FactoryPal/API/data_source/metrics.json")
# print(df[df["id"] == 0])
