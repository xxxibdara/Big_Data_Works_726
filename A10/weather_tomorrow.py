import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from datetime import date
from pyspark.sql import SparkSession, types
from pyspark.ml import PipelineModel


def main(model_file):
    tmrw_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.DateType()),
        types.StructField('latitude', types.FloatType()),
        types.StructField('longitude', types.FloatType()),
        types.StructField('elevation', types.FloatType()),
        types.StructField('tmax', types.FloatType())
    ])

    tmrw_data = spark.createDataFrame([('SFU lab', date(2022, 11, 18), 49.2771, -122.9146, 330.0, 12.0), \
        ('SFU lab', date(2022, 11, 19), 49.2771, -122.9146, 330.0, 8.0)], schema = tmrw_schema)
    model = PipelineModel.load(model_file)
    predictions = model.transform(tmrw_data).collect()
    print(predictions)

    prediction = predictions[0]['prediction']

    print('Predicted tmax tomorrow:', prediction)
   

if __name__ == '__main__':
    spark = SparkSession.builder.appName('weather tomorrow').getOrCreate()
    assert spark.version >= '2.3' # make sure we have Spark 2.3+
    spark.sparkContext.setLogLevel('WARN')
    model_file = sys.argv[1]
    main(model_file)