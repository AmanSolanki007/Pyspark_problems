from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql import functions as func
spark = SparkSession.builder.appName("myapp").getOrCreate()

schema = StructType([StructField("id",IntegerType(),True),
                     StructField("name",StringType(),True)])

data = spark.read.schema(schema).option("sep"," ").csv("Marvel-names.txt")
data.printSchema()

lines = spark.read.option("inferschema","True").text("Marvel-graph.txt")

connections = lines.withColumn("id",func.split(func.col("value")," ")[0]) \
    .withColumn("connections",func.size(func.split(func.col("value")," "))-1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

connections.show()

mostpopular = connections.sort(func.col("connections").desc()).first()

mostpopular_name = data.filter(func.col("id")==mostpopular[0]).select(func.col("name")).first()

print(mostpopular_name[0])

