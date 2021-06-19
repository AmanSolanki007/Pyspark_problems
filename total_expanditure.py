from pyspark import SparkContext
sc = SparkContext("local","Total_count")
def lineparcer(line):
    fields = line.split(",")
    cust_id = int(fields[0])
    expenditure = float(fields[2])
    return(cust_id,expenditure)

data = sc.textFile("customer-orders.csv")
processed_data = data.map(lineparcer)
"This is the result which is sorted by cust id"
results = processed_data.reduceByKey(lambda x,y:x+y).sortByKey()
"flipped is sorted by the amount which is spand by a customers"
flipped = results.map(lambda x:(x[1],x[0])).sortByKey().collect()
for result in flipped:
    print("Total Expenditure by user:-",result)