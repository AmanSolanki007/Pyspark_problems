from pyspark.sql import SparkSession
from pyspark.sql import functions as fs

spark= SparkSession.builder.appName("word_count").getOrCreate()

data= spark.read.text("book.txt")

pro_data= data.select(fs.explode(fs.split(data.value,"\\W+")).alias("words"))
pro_data.filter(pro_data.words !="")

a=pro_data.select("words").groupBy("words").count().show()
