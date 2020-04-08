import os
from pyspark import SparkConf, SparkContext

# export JAVA_HOME=$(/usr/libexec/java_home -v 1.8) && python3 popular_superhero.py
os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3.7"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/local/bin/python3.7"

def count_occurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements)-1)


def parse_names(lines):         
    fields = lines.split('\"')
    return (int(fields[0]), fields[1].encode("utf-8"))


conf = SparkConf().setMaster("local").setAppName("MostPopularHero")
sc = SparkContext(conf = conf)

names = sc.textFile("file:////Users/felix.negoita/Desktop/others/spark_tutorial/Marvel-Names.txt")
names_rdd = names.map(parse_names)
lines = sc.textFile("file:////Users/felix.negoita/Desktop/others/spark_tutorial/Marvel-Graph.txt")
pairs = lines.map(count_occurences)
total_by_chars = pairs.reduceByKey(lambda x, y: x + y)
total_flipped = total_by_chars.map(lambda x: (x[1], x[0]))

most_popular = total_flipped.max()
name = names_rdd.lookup(most_popular[1])[0]

print("Most popular Marvel superhero by comics", name, "with {} occurences with other characters".format(most_popular[0]))
