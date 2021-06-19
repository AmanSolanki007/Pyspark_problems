from pyspark import SparkContext

sc = SparkContext("local","max-temp")

data = sc.textFile("Spark_problems/datasets/1800.csv")

def lineparser(line):
    fields = line.split(',')
    station_id = fields[0]
    entry_type = fields[2]
    temperatures = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return(station_id,entry_type,temperatures)

clean_data = data.map(lineparser)
max_temp = clean_data.filter(lambda x:"TMAX" in x[1])





