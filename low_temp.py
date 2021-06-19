from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, StructField


spark = SparkSession.builder.appName("min_temp").getOrCreate()

schema = StructType([
    StructField("station_id",StringType(),True),
    StructField("date",IntegerType(),True),
    StructField("measure_type",StringType(),True),
    StructField("temperature",FloatType(),True)])

data = spark.read.schema(schema).csv("1801.csv")
min_data = data.filter(data.measure_type=="TMIN")
data2=min_data.select("station_id","temperature")
data2.show()

result = data2.groupBy("station_id").min("temperature").sort("temprature").show()

# with open("1801.csv","w") as f :
#     csvWriter = csv.writer(f)
#     for i in df:
#         csvWriter.writerow(i)
