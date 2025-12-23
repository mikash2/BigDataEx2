import sys
from pyspark import SparkContext, SparkConf

def count_words_in_line(text_file):
    lines_with_counts = text_file.map(lambda line: (line, len(line.split(" "))))
    max_line = lines_with_counts.reduce(lambda l1, l2: l1 if l1[1] >= l2[1] else l2)
    return max_line

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: count_words_in_line ", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("count-words-in-line")
    sc = SparkContext(conf=conf)
    text_file = sc.textFile("s3a://" + sys.argv[1])
    
    max_line = count_words_in_line(text_file)

    print("--------------------------------------------")
    print("Line with the most words:")
    print(max_line[0])
    print("Number of words:", max_line[1])
    print("--------------------------------------------")
