from pyspark import SparkContext

sc = SparkContext("local","Word_count")

data = sc.textFile("book.txt")

split_data = data.flatMap(lambda x:x.split())
map_data = split_data.map(lambda x:(x,1))
counting = map_data.reduceByKey(lambda x,y:(x+y)).collect()
print("this is the result",counting)

