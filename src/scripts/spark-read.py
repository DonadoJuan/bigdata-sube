from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().set("spark.driver.memory", "10g") \
    .set("spark.debug.maxToStringFields", "10000") \
    .set("spark.driver.maxResultSize", "1g") \
    .setMaster("local[*]") \
    .setAppName("processCSV")

csv = "hdfs://kudu:20500/up/csv"

sc = SparkContext(conf=conf)
sp = SQLContext(sc)
df = sp.read.csv("data/data-source.csv", header=True)
df.show()
