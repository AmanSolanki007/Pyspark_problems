from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,StringType,IntegerType



spark = SparkSession.builder.appName("mydata").getOrCreate()

data = spark.read.option("inferschema","true").csv("customer-orders.csv")
s_data = data.select("_c0","_c2").toDF("customer_id","expenditure")

total_ex= s_data.groupBy("customer_id").agg(func.round(func.sum("expenditure")).alias("total_expenditure")).orderBy("customer_id").show()
