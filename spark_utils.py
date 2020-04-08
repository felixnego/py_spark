import os
from pyspark import SparkConf, SparkContext

# export JAVA_HOME=$(/usr/libexec/java_home -v 1.8) && python3 ratings-counter.py

os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3.7"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/local/bin/python3.7"

conf = SparkConf().setMaster("local").setAppName("")
sc = SparkContext(conf = conf)
