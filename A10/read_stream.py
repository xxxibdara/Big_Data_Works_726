import sys
from pyspark.sql import SparkSession, functions
#from Kafka import KafkaConsumer

assert sys.version_info >= (3,5) # make sure we have Python 3.5+


def main(topic):

    messages = spark.readStream.format('kafka') \
            .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
            .option('subscribe', topic).load()

    values = messages.select(messages['value'].cast('string'))
    values = values.select(functions.split(values['value'], ' ').alias('x_and_y'))
    with_xy = values.withColumn('x', values['x_and_y'].getItem(0).cast('float'))\
        .withColumn('y', values['x_and_y'].getItem(1).cast('float'))
    with_n = with_xy.withColumn('xy', with_xy['x']*with_xy['y']).withColumn('x^2', with_xy['x']**2).withColumn('n', functions.lit(1))
    summary = with_n.groupBy().sum()


    slope = summary.withColumn('beta', (summary['sum(xy)']-(summary['sum(x)']*summary['sum(y)']/summary['sum(n)']))\
        /(summary['sum(x^2)']-(summary['sum(x)']**2)/summary['sum(n)']))
    intercept = slope.withColumn('alpha', (slope['sum(y)']/slope['sum(n)'])-slope['beta']*(slope['sum(x)']/slope['sum(n)']))

    stream_df = intercept.select(intercept['beta'], intercept['alpha'])
    stream = stream_df.writeStream.format('console').outputMode('complete').start()

    stream.awaitTermination(600)

    #consumer = KafkaConsumer(topic, bootstrap_servers=['node1.local', 'node2.local'])
    #for msg in consumer:
    #    print(msg.value.decode('utf-8'))

if __name__ == '__main__':
    spark = SparkSession.builder.appName('read stream').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    topic = sys.argv[1]
    main(topic)