
#quality.py → Data Validation Layer
#Schema checks
#Null checks
#Data rules



from config import expected_cols
from pipeline import df,bronze_count, silver_count

print("Expected:", expected_cols)
def schema_check(df,expected_cols):
    actual_cols = df.columns
    missing_cols = [c for c in expected_cols if c not in actual_cols]
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")
    print("✅ Schema check passed")
schema_check(df, expected_cols)



def null_check(df,columns):
    errors = []
    for col in columns:
        null_count=df.filter(df[col].isNull()).count()
        print(f"Null values in {col}: {null_count}")
        if null_count > 0:
           errors.append(col)
    if errors:
        raise ValueError(f"Columns with null values: {errors}")
null_check(df, expected_cols)

#his catches if your transform accidentally deleted too much data (more than 5%)

def row_count_check(bronze_count, silver_count, threshold=0.05):
    if bronze_count == 0:
        raise ValueError("Bronze count is zero, cannot perform row count check.")
    drop_pct = (bronze_count - silver_count) / bronze_count
    print(f"Row count drop percentage: {drop_pct:.2%}")
    if drop_pct > threshold:
        raise ValueError(f"Row count dropped by {drop_pct:.2%}, which exceeds the threshold of {threshold:.2%}.")
    print("✅ Row count check passed")
    print(f"Bronze count: {bronze_count}, Silver count: {silver_count}",drop_pct)
row_count_check(bronze_count, silver_count)