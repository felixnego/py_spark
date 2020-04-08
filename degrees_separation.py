import os
from pyspark import SparkConf, SparkContext

# export JAVA_HOME=$(/usr/libexec/java_home -v 1.8) && python3 degrees_separation.py

os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3.7"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/local/bin/python3.7"

conf = SparkConf().setMaster("local").setAppName("")
sc = SparkContext(conf = conf)

start_char_id = 5306
target_char_id = 14

# accumulator, to signal when the target is found
hit = sc.accumulator(0)

def convert_to_bfs(line):
    # initiliaze the graph
    fields = line.split()
    hero_id = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))
    color = 'white'
    distance = 9999
    if hero_id == start_char_id:
        color = 'gray'
        distance = 0
    return (hero_id, (connections, distance, color))


def create_starting_rdd():
    input = sc.textFile("file:///Users/felix.negoita/Desktop/others/spark_tutorial/Marvel-Graph.txt")
    return input.map(convert_to_bfs)


def bfs_map(node):
    hero_id = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    if color == 'gray':
        for connection in connections:
            new_hero_id = connection
            new_distance = distance + 1
            new_color = 'gray'
            if target_char_id == connection:
                hit.add(1)

            new_entry = (new_hero_id, ([], new_distance, new_color))
            results.append(new_entry)

        color = 'black'

    results.append((hero_id, (connections, distance, color)))
    return results


def bfs_reduce(data1, data2):
