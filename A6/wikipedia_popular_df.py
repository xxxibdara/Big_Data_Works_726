import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    path_split = path.split('/')
    ex_from = len('pagecounts-')
    ex_to = ex_from + len('20160801-12')
    return path_split[-1][ex_from:ex_to] #format YYYYMMDD-HH



def main(inputs, output):
    wikipedia_schema = types.StructType([
        types.StructField('language', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('views', types.IntegerType()),
        types.StructField('bytes', types.IntegerType())
    ])

    wikipedia = spark.read.csv(inputs, sep = ' ', schema = wikipedia_schema).withColumn('filename', functions.input_file_name())
    wiki_with_hour = wikipedia.withColumn('hour', path_to_hour(wikipedia['filename'])) #add a column with its time: YYYYMMDD-HH

    filtered = wiki_with_hour.filter((wiki_with_hour['language'] == 'en') & (wiki_with_hour['title'] != "Main Page") & (wiki_with_hour['title'].startswith("Special:") == False))
    wiki = filtered.cache()

    max_views = wiki.groupby('hour').agg(functions.max(wiki['views']).alias('views'))
    #joined = max_views.join(wiki, ['hour', 'views'],'inner') #without broadcast hint
    joined = wiki.join(max_views.hint('broadcast'), ['hour', 'views'])
    output_data = joined.select(joined['hour'], joined['title'], joined['views']).orderBy(joined['hour'], joined['title'])
    
    output_data.write.json(output, mode = 'overwrite')
    output_data.explain()


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('wikipedia popular df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
