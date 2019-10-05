from __future__ import print_function
from pyspark import SparkContext,SparkConf


if __name__ == "__main__":
    conf = SparkConf().setAppName('wordCounter').setMaster("local[*]")
    sc = SparkContext(conf=conf)  # SparkContext
    document = sc.textFile('texts/*/*')
    words = document.flatMap(lambda line: line.replace(".\n", " ").replace(",", " ").replace(". ", " ").replace("(", " ").replace(")"," ").replace('"', ' ').replace("[", " ").replace("]", " ").replace(";", " ").replace(".", " ").split(' '))
    wordCounts = words.countByValue()

    for word, count in wordCounts.iteritems():
        print('{} : {}'.format(word, count))
    sc.stop()