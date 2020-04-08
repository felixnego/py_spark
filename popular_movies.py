import os
from pyspark import SparkConf, SparkContext

# export JAVA_HOME=$(/usr/libexec/java_home -v 1.8) && python3 popular_movies.py
os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3.7"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/local/bin/python3.7"

def load_movie_names():
    """
    Crates and returns a dictionary that reads the u.item file
    and maps movie ids to movie names
    """
    movie_names = {}
    with open('ml-100k/u.item', encoding="ISO-8859-1") as file:
        for line in file:
            fields = line.split("|")
            movie_names[int(fields[0])] = fields[1]
    return movie_names


conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)
names_dict = sc.broadcast(load_movie_names())  # call .value to retrieve the
# dictionary from the spark broadcast object

input = sc.textFile("file://///Users/felix.negoita/Desktop/others/spark_tutorial/ml-100k/u.data")
movies = input.map(lambda x: (int(x.split()[1]), 1))
movie_counts = movies.reduceByKey(lambda x, y: x + y)
flipped = movie_counts.map(lambda x: (x[1], x[0]))
flipped_sorted = flipped.sortByKey()

sorted_names = flipped_sorted.map(lambda x: (names_dict.value[x[1]], x[0]))

results = sorted_names.collect()
for result in results:
    print(result)
