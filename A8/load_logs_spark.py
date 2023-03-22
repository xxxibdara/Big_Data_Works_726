from pyspark.sql import SparkSession, types
from cassandra.cluster import Cluster
import sys, re, uuid

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def parse_line(input):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    line = line_re.split(input)
    if len(line) > 4:
        host = line[1]
        path = line[3]
        bytes = int(line[4])
        id = str(uuid.uuid4())
        return host, path, bytes, id

def main(input, output_kys, nasalogs):

    data = sc.textFile(input).repartition(100)
    re_data = data.map(parse_line).filter(lambda x: x is not None) #filter out the None tuples
    nasa_schema = types.StructType([
        types.StructField('host', types.StringType()),
        types.StructField('path', types.StringType()),
        types.StructField('bytes', types.IntegerType()),
        types.StructField('id', types.StringType())
    ])
    df = spark.createDataFrame(re_data, schema = nasa_schema)
    df.show(5)
    df.write.format("org.apache.spark.sql.cassandra").mode('overwrite').option('confirm.truncate','true') \
        .options(table = nasalogs, keyspace = output_kys).save()

if __name__ == "__main__":
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('load logs spark') \
                .config('spark.cassandra.connection.host',','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    input = sys.argv[1]
    output_kys = sys.argv[2]
    table = sys.argv[3]
    main(input, output_kys, table)
    

