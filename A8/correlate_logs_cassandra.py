from pyspark.sql import SparkSession, types, functions
from cassandra.cluster import Cluster
import sys, re, math

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def main(input_kys, table):

    nasa = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table=table, keyspace=input_kys).load()


    data_points = nasa.groupby(nasa['host']).agg(functions.count(nasa['path']).alias('x'), functions.sum(nasa['bytes']).alias('y'))
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



if __name__ == "__main__":
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('correlation logs cassandra') \
                .config('spark.cassandra.connection.host',','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    input_kys = sys.argv[1]
    table = sys.argv[2]
    main(input_kys, table)