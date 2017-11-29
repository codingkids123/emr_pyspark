from __future__ import print_function

import sys
from operator import add

from pipeline import pipeline


@pipeline('word_count')
def word_count(spark, filename):
    lines = spark.read.text(filename).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()


if __name__ == "__main__":
    word_count(sys.argv[1])
