from __future__ import print_function
import os
from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF, IDF


if __name__ == "__main__":
    sc = SparkContext(appName="TFIDF")  # SparkContext
    for path, subdirs, files in os.walk('texts'):
        for name in files:
            if not name.startswith("."):
                document = sc.textFile(os.path.join(path, name)).map(lambda line: line.split(" "))
                hashingTF = HashingTF()
                tf = hashingTF.transform(document)
                tf.cache()

                idf = IDF().fit(tf)
                tfidf = idf.transform(tf)

                idfIgnore = IDF(minDocFreq=2).fit(tf)
                tfidfIgnore = idfIgnore.transform(tf)

                tfidf.saveAsTextFile("vectors/tfidf/"+path.split("/")[1]+"/"+name)
                tfidfIgnore.saveAsTextFile("vectors/tfidfIgnore/"+path.split("/")[1]+"/"+name)
    sc.stop()
