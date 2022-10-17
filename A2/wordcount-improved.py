from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count improved')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+


def words_once(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation)) # word seperated
    for w in re.split(wordsep,line): 
        yield (w.lower(), 1) # convert key to lower case

def add(x, y):
    return x + y

def get_key(kv): 
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

text = sc.textFile(inputs)
words = text.flatMap(words_once).filter(lambda x: x[0] != "") 

wordcount = words.reduceByKey(add)

outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)