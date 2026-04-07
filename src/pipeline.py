
from pyspark.sql import SparkSession
import logging
logging.basicConfig(level=logging.INFO)

def get_spark():
    spark = SparkSession.builder.appName("NYCTaxiPipeline").getOrCreate()#.config("spark.hadoop.io.native.lib", "false").config("spark.hadoop.io.native.lib.available", "false")
    return spark
def extract(spark, path):
    df = spark.read.parquet(path)
    return df
spark = get_spark()
df = extract(spark, "data/yellow_tripdata_2023-01.parquet")
df.show(5)

bronze_count=df.count()#raw count before any transformations
logging.info(f"Bronze count: {bronze_count}")

df.write.mode("overwrite").parquet("output/bronze/")#we store or write data in parquet format in the output/bronze/ directory. We use overwrite mode to replace any existing data in that directory.
logging.info("[BRONZE] Bronze data written to output/bronze/")#We log an informational message indicating that the bronze data has been written to the output/bronze/ directory. This helps us track the progress of our data pipeline and confirms that the write operation was successful.



#logging.info("[BRONZE] Written to output/bronze/")
#| Problem              | Fix                                |
#| -------------------- | ---------------------------------- |
#| winutils error       | Install winutils + set HADOOP_HOME |
#| write failing        | due to missing Hadoop utils        |
                    

#PHASE 3 — Silver Layer

from pyspark.sql.functions import avg, col,month,round

def transform(df):
    df=df.dropna(subset=["fare_amount","PULocationID"]).filter(col("fare_amount")>0).filter(col("PULocationID")>0)
    df=df.withColumn('tip_pct',round((col("tip_amount")/col("fare_amount"))*100,2))
    df=df.withColumn("month",month(col("tpep_pickup_datetime")))
    return df

df2=transform(df)
silver_count=df2.count()#count after transformations
logging.info(f"Silver count: {silver_count}")   


#cache
import time
#from quality import null_check, row_count_check
#from config import expected_cols
silver=transform(df)
silver.cache()
t1=time.time()
silver_count=silver.count()
#null_check(silver, expected_cols)
#row_count_check(bronze_count, silver_count)
t2=time.time()
logging.info(f"Quality checks passed in {t2-t1:.2f} seconds")
#silver.write.mode("overwrite").parquet("output/silver/")
silver.unpersist()



from pyspark.sql.functions import avg,count,sum as spark_sum


def aggregate(df):
    return df.groupBy('PULocationID').agg(
        avg("fare_amount").alias("avg_fare"),
        avg('tip_pct').alias("avg_tip_pct"),
        count("*").alias("trip_count"),
        spark_sum("fare_amount").alias("total_revenue")
    )
gold=aggregate(silver)
gold.show(5)

#Gold quality checks
#assert condition, "Error message"
#If True → program continues ✅
#If False → program stops immediately with an error ❌
assert gold.count()>0,"gold layer is empty"

assert gold.filter(col("avg_fare")<0).count()==0,"Negative average fare found in gold layer"
print("All quality checks passed for gold layer")




gold.write.mode('overwrite').parquet('output/gold')
gold.createOrReplaceTempView('gold_trips')
#result=spark.sql("with ranked as (select * ,rank() over (order by total_revenue desc) as rev_rank from gold_trips)select * from ranked where rev_rank<= 10")
result = spark.sql("""
WITH ranked AS (
    SELECT *,
           RANK() OVER (ORDER BY total_revenue DESC) AS rev_rank
    FROM gold_trips
)
SELECT * FROM ranked WHERE rev_rank <= 10
""")
result.show()
logging.info("[GOLD] top 10 zones by revenue shown above")


from pyspark.sql.functions import broadcast

zone_lookup = spark.read.csv("data/taxi_zone_lookup.csv", header=True, inferSchema=True)

joined=silver.join(zone_lookup,silver.PULocationID==zone_lookup.LocationID,"left")


joined=silver.join(broadcast(zone_lookup),silver.PULoctionID == zone_lookup.LocationID,"left")