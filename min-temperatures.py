"""from pyspark import SparkContext

sc = SparkContext("local","min_temp")
data = sc.textFile("Spark_problems/datasets/1800.csv")

def lineparcer(line):
    fields = line.split(",")
    station_id=fields[0]
    entry_type=fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return(station_id,entry_type,temperature)

parseline = data.map(lineparcer)

min_data= parseline.filter(lambda x: "TMIN" in x[1])
station_temp = min_data.map(lambda x : (x[0],x[2]))
min_temp = station_temp.reduceByKey(lambda x,y:min(x,y))
result = min_temp.collect()

for result in result:
    print(result)
"""

from pyspark import SparkConf, SparkContext
import math

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("Spark_problems/datasets/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

