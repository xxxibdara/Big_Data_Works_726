from pyspark import SparkConf, SparkContext
import sys
import random
import operator


assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

batches = 100

def total(samples):
    random.seed() #to avoid the same random seed
    iterations = 0
        # group your experiments into batches to reduce the overhead
    for i in range(samples):
        sum = 0.0
        while sum < 1:
            sum += random.random() # Return the next random floating point number in the range [0.0, 1.0).
            iterations += 1
    return iterations


def main(inputs):
    samples = sc.parallelize([inputs], numSlices = batches).map(total)
    total_itertations = samples.reduce(operator.add)

    print(total_itertations/inputs)

if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = int(sys.argv[1])
    main(inputs)
    






