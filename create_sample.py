import pandas as pd 
df=pd.read_parquet("data/yellow_tripdata_2023-01.parquet")


print(f"full dataset : {len(df)} rows")

df.head(1000).to_parquet("data/sample_1000rows.parquet")

print("sample dataset created with 1000 rows ✅")