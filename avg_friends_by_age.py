from pyspark import SparkContext
sc = SparkContext("local","FriendsbyAGE")
rdd = sc.textFile("Spark_problems/datasets/fakefriends.csv")

def lineparcer(line):
    fields = line.split(',')
    age = int(fields[2])
    num_of_friends = int(fields[3])
    return(age,num_of_friends)

data= rdd.map(lineparcer)

totalsbyAGE = data.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
averagesByAge = totalsbyAGE.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)

