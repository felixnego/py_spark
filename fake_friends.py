import os
from pyspark import SparkConf, SparkContext

# export JAVA_HOME=$(/usr/libexec/java_home -v 1.8) && python3 fake_friends.py

def parse_lines(line):
    lines = line.split(",")
    age = int(lines[2])
    num_friends = int(lines[3])

    return (age, num_friends)


os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3.7"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/local/bin/python3.7"
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)


lines_rdd = sc.textFile("file:////Users/felix.negoita/Desktop/others/spark_tutorial/fakefriends.csv")
parsed_rdd = lines_rdd.map(parse_lines)

# for data_point in lines_rdd.collect():
#     print(data_point)
print(lines_rdd.count())
