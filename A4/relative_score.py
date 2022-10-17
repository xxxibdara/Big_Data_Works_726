from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def parse_json(input):
    info = json.loads(input) # parses a JSON string to a Python object
    yield (info["subreddit"], (1, info["score"], info["author"])) #(subreddit, (counts, score_sum, author))

def get_key(kv):
    return kv[0]

def add_pairs(a,b):
    return (a[0]+b[0],a[1]+b[1]) 

def get_averages(scs):
    subreddit, (counts, scores) = scs
    return  (subreddit, (scores/counts))

def relative_score(kv):
    subreddit, ((score, author), average) = kv
    return (score/average, author)


def main(inputs, output):
    
    text = sc.textFile(inputs)
    commentdata = text.flatMap(parse_json).cache()
    
    averages_pairs = commentdata.reduceByKey(add_pairs).map(get_averages).filter(lambda x: x[1] > 0).sortBy(get_key) #(subreddit, averages)
    
    #(subreddit, ((score, author), average)) eg. ('scala', ((0, 'Hanxarin'), 2.224945770065076))
    joined = commentdata.map(lambda x: (x['subreddit'], (x['score'], x['author']))).join(averages_pairs) 
    
    author_comment = joined.map(relative_score).sortBy(get_key, ascending = False)
    author_comment.map(json.dumps).saveAsTextFile(output)



if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
