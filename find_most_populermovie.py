from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("populer_movie").getOrCreate()
schema = StructType([StructField("user_id",StringType(),True),
                     StructField("movie_id",StringType(),True),
                     StructField("ratings",IntegerType(),True),
                     StructField("time_stamp",IntegerType(),True)])

data = spark.read.option("sep","\t").schema(schema).csv("u.data")

populer_movie = data.groupBy("movie_id").count().orderBy(func.desc("count"))
populer_movie.show()


