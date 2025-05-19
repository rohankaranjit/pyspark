import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col ,explode

spark = SparkSession.builder.appName("Youtube Data").getOrCreate()

df = spark.read.csv("MXvideos.csv",header = True)

df = df.withColumn("country" ,lit("MX"))

df.select("title","views","category_id","country").show(10)