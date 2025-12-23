import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount ", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("python-word-count")
    sc = SparkContext(conf=conf)
    text_file = sc.textFile("hdfs://" + sys.argv[1])
    raw_words = text_file.flatMap(lambda line: line.split(" ")).cache()
    distinct_words_count = raw_words.distinct().count()

    counts = raw_words.map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .repartition(5) \
        .filter(lambda x: len(x[0]) > 5)

    list = counts.takeOrdered(40, key=lambda x: -x[1])
    print("--------------------------------------------")
    print(*list, sep="\n")
    print("--------------------------------------------")
    print("Total distinct words (all words):", distinct_words_count)
