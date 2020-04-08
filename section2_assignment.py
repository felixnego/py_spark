import os
from pyspark import SparkConf, SparkContext
# export JAVA_HOME=$(/usr/libexec/java_home -v 1.8) && python3 section2_assignment.py
os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3.7"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/local/bin/python3.7"

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def extract_relevant(row):
    row = row.split(',')
    return row[0], row[2]


input = sc.textFile('file:///Users/felix/Desktop/coding/spark/customer-orders.csv')
id_price = input.map(extract_relevant)
amount_by_customer = id_price.reduceByKey(lambda x, y: float(x) + float(y))
sorted = amount_by_customer.map(lambda x: (x[1], x[0])).sortByKey()


results = sorted.collect()
for data_row in results:
    print(data_row[1], "spent", data_row[0])
