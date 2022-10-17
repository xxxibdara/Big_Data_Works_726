from pyspark import SparkConf, SparkContext
import sys

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def get_parsed(line):
    words = line.split()
    return (words[0],words[1],words[2],int(words[3]),words[4]) #convert view count to an integer


def get_filtered(line):
   if (line[1] == "en"):
      page_name = line[2] #the page name 
      if(page_name != "Main_Page" and not page_name.startswith("Special:")):
         return True
   

def get_key(kv):
   return kv[0]
   
def get_pair(kv):
   return (kv[0], (kv[3], kv[2]))

def get_max(x,y):
	if (x[0] > y[0]):
		return x
	return y


def tab_separated(kv):
    a, (b, c) = kv
    return "%s\t (%s, '%s')" % (a, b, c)


text = sc.textFile(inputs) # RDD of strings
page_view_info = text.map(get_parsed)

records = page_view_info.filter(get_filtered) # remove the records we don't consider
pairs = records.map(get_pair) # (time, (view_counts, page_name))

max_pair = pairs.reduceByKey(get_max) # reduce to find the max value for each key

max_count = max_pair.sortBy(get_key) # sort by key(time)
max_count.map(tab_separated).saveAsTextFile(output)