import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types



def main(inputs, output):
    temp_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType())
        
    ])

    weather_station = spark.read.csv(inputs, schema = temp_schema)
    weather_station.createOrReplaceTempView('weather_station')
    filtered = spark.sql("""
        SELECT * FROM weather_station 
        WHERE qflag IS NULL AND (observation == 'TMAX' OR observation == 'TMIN')
    """)
    filtered.createOrReplaceTempView('filtered')

    min_table = spark.sql("""
        SELECT date, station, value AS value_min  FROM filtered
        WHERE observation == 'TMIN'
    """)
    min_table.createOrReplaceTempView('min_table')
    max_table = spark.sql("""
        SELECT date, station, value AS value_max  FROM filtered
        WHERE observation == 'TMAX'
    """)
    max_table.createOrReplaceTempView('max_table')
    combined = spark.sql("""
        SELECT a.date, a.station, value_max, value_min  FROM min_table i
        JOIN max_table a ON a.date = i.date AND a.station = i.station
    """)
    combined.createOrReplaceTempView('combined')

    range = spark.sql("""
        SELECT date, station, ((value_max-value_min)/10) AS range 
        FROM combined
    """)
    range.createOrReplaceTempView('range')

    range = range.cache()
    max_range = spark.sql("""
        SELECT date, MAX(range) AS range 
        FROM range
        GROUP BY date
    """)
    max_range.createOrReplaceTempView('max_range')
    
    range_join = spark.sql("""
        SELECT m.date AS date, r.station AS station, m.range AS range
        FROM max_range m JOIN range r
        ON r.date = m.date AND r.range = m.range
        ORDER BY m.date, r.station
    """)
    
    range_join.write.csv(output, mode = 'overwrite')





if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range sql').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)