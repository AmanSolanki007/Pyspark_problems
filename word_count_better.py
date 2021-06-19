from pyspark import SparkContext
import re
sc = SparkContext("local","Word_count")
data = sc.textFile("book.txt")

def lineparser(text):
    return re.compile(r"\W+",re.UNICODE).split(text.upper())

process_data = data.flatMap(lineparser)
word_count=process_data.countByValue()
for word , count in word_count.items():
    cleanword = word.encode("ascii","ignore")
    if(cleanword):
        print(cleanword,count)