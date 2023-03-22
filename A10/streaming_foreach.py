import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('streaming example').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')


def process_batch(df, epoch_id):
    print('Epoch ID:', epoch_id)
    df.show()


def main1():
    lines = spark.readStream.format('text') \
            .option('path', '/tmp/text').load()

    lengths = lines.select(
        functions.length(lines['value']).alias('len')
    )
    
    stream = lengths.writeStream.foreachBatch(process_batch).start()
    stream.awaitTermination(600)


def process_row(row):
    print('observed row:', row)


def main2():
    lines = spark.readStream.format('text') \
            .option('path', '/tmp/text').load()

    lengths = lines.select(
        functions.length(lines['value']).alias('len')
    )
    
    stream = lengths.writeStream.foreach(process_row).start()
    stream.awaitTermination(600)


if __name__ == '__main__':
    main1()