from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sparkSQL").getOrCreate()

def lineparser(line):
    fields = line.split(',')
    id = int(fields[0])
    name = str(fields[1].encode("utf-8"))
    age = int(fields[2])
    numfriends = int(fields[3])
    return(id,name,age,numfriends)

lines = spark.sparkContext.textFile('fakefriends.csv')
pro_data = lines.map(lineparser)

df = spark.createDataFrame(pro_data).toDF("id","name","age","num_friends")

df.createOrReplaceTempView("pro_data")

query = spark.sql("select * from pro_data where age=31").show()

df.groupBy("age").count().orderBy("age").show()


