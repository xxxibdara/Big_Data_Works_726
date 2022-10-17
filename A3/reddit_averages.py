from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def parse_json(input):
    info = json.loads(input) # parses a JSON string to a Python object
    yield (info["subreddit"], (1, info["score"])) #(subreddit, (counts, score_sum))

def get_key(kv):
    return kv[0]

def add_pairs(a,b):
    return (a[0]+b[0],a[1]+b[1]) 

def get_averages(scs):
    subreddit, (counts, scores) = scs
    return  (subreddit, (scores/counts))


def main(inputs, output):
    text = sc.textFile(inputs)
    reddit_info = text.flatMap(parse_json)
    
    pairs = reddit_info.reduceByKey(add_pairs)
    averages = pairs.map(get_averages).sortBy(get_key)
    results = averages.map(json.dumps)
    results.saveAsTextFile(output)



if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)