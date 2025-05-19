import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col ,lit,explode

spark = SparkSession.builder.appName("All COuntry Trending").getOrCreate()

countries = ["CA", "DE", "FR", "GB", "IN", "JP", "KR", "MX", "RU", "US"]
dfs =[]

for country in countries:
    path = (f"{country}videos.csv")
    df = spark.read.csv(path , header = True)
    df = df.withColumn("country",lit(country))
    dfs.append(df)

full_df = dfs[0]
for df in dfs[1:]:
    full_df = full_df.unionByName(df)
full_df.select("title", "views", "country").distinct().show(10, truncate = False)

