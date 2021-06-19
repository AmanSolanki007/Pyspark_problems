from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField,StringType,DateType,IntegerType

spark = SparkSession.builder.appName("assignment").getOrCreate()
data = spark.sparkContext.textFile("customer_20210523_111025.txt")
#function for data preprocessing
def lineparser(line):
    fields=line.rstrip().split(",")
    sp = str(fields).split("|")
    Name = sp[2]
    Customer_Id = sp[3]
    Open_Date = sp[4]
    Last_Consulted_Date =sp[5]
    Vaccination_Id = sp[6]
    Dr_Name = sp[7]
    State = sp[8]
    Country=sp[9]
    DOB=sp[10]
    Is_Active=sp[11]
    return(Name,Customer_Id,Open_Date,Last_Consulted_Date,Vaccination_Id,Dr_Name,State,Country,DOB,Is_Active)
# apply function on RDD
Preprocess_data = data.map(lineparser)
print(Preprocess_data.collect())

# Create a data frame from RDD
schema = StructType([StructField("Customer_Name",StringType(),True),
                    StructField("Customer_Id",IntegerType(),True),
                    StructField("Open_Date",DateType(),True),
                    StructField("Last_Consulted_Date",DateType(),True),
                    StructField("Vaccination_Id",IntegerType(),True),
                    StructField("Dr_Name",StringType(),True),
                    StructField("State",StringType(),True),
                    StructField("Country",StringType(),True),
                    StructField("DOB",DateType(),True),
                    StructField("Is_Active",StringType(),True)])

df = spark.createDataFrame(Preprocess_data)

df.show()




