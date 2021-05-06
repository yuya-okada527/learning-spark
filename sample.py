import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
sc = pyspark.SparkContext(appName='hello_spark')
spark = SparkSession(sc).builder.getOrCreate()
sample = ['Hello', 'Spark', 'World']
rdd = sc.parallelize(sample)
print(rdd.collect())
spark.stop()