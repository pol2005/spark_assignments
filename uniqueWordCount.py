from __future__ import print_function
import os
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName('wordCounter').setMaster("local[*]")
    sc = SparkContext(conf=conf)  # SparkContext


    for path, subdirs, files in os.walk('texts'):
        for name in files:
            if not name.startswith("."):
                print(os.path.join(path, name))
                document = sc.textFile(os.path.join(path, name))
                words = document.flatMap(lambda line: line.replace(".\n", " ").replace(",", " ").replace(". ", " ").replace("(", " ").replace(")", " ").split(' '))
                wordCounts = words.countByValue()
                print(len(wordCounts))
                for word, count in wordCounts.iteritems():
                    print ('{} : {}'.format(word, count))

    sc.stop()