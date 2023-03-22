import math
import re 
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def parse_line(input):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    line = line_re.split(input)
    if len(line) > 4:
        return line[1], line[2], line[3], int(line[4])

def main(inputs):

    nars_schema = types.StructType([
        types.StructField('hostname', types.StringType()),
        types.StructField('datetime', types.StringType()),
        types.StructField('path', types.StringType()),
        types.StructField('bytes', types.IntegerType())
    ])

    data = sc.textFile(inputs).map(parse_line).filter(lambda x: x is not None) #filter out the None tuples
    nars = spark.createDataFrame(data, schema = nars_schema)
    nars.show(10)
    data_points = nars.groupby(nars['hostname']).agg(functions.count(nars['path']).alias('x'), functions.sum(nars['bytes']).alias('y'))
    six_values = data_points.withColumn('n', functions.lit(1)).withColumn('x_sqr', pow(data_points['x'], 2)).withColumn('y_sqr', pow(data_points['y'], 2)).withColumn('xy', data_points['x']*data_points['y'])
    six_values.show(10)
    six_sum = six_values.groupby().sum()
    six_sum.show()
    six_sum =  six_sum.collect()
    

    x = six_sum[0][0]
    y = six_sum[0][1]
    n = six_sum[0][2]
    x_sqr = six_sum[0][3]
    y_sqr = six_sum[0][4]
    xy = six_sum[0][5]
    r = (n*xy - x*y)/ (math.sqrt(n*x_sqr - pow(x, 2))* math.sqrt(n*y_sqr - pow(y, 2)))
    r_sqr = pow(r, 2)
    print("r = "+ str(r)+",", "r^2 = " + str(r_sqr))


if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('correlate logs').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)