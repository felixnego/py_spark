import os
import re
from pyspark import SparkConf, SparkContext
# export JAVA_HOME=$(/usr/libexec/java_home -v 1.8) && python3 word_count.py
os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3.7"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/local/bin/python3.7"


def normalize_words(line):
    return re.compile(r'\W+', re.UNICODE).split(line.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file://///Users/felix/Desktop/coding/spark/Book.txt")
words = input.flatMap(normalize_words)
# word_count = words.countByValue()  # this returns a python object, not an rdd
word_count = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
word_count_sorted = word_count.map(lambda x: (x[1], x[0])).sortByKey()

# for word, count in word_count.items():
#     # clean_word = word.encode('ascii', 'ignore')
#     # if clean_word:
#     # goes to binary in python3
#         print(word, ":", count)

results = word_count_sorted.collect()

for result in results:
    count = str(result[0])
    word = result[1]
    print(word, "\t\t", count)
