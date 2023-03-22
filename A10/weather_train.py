import sys
from pyspark.sql import SparkSession, types
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator


assert sys.version_info >= (3, 5) # make sure we have Python 3.5+




def main(inputs, model_file):
    tmax_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.DateType()),
        types.StructField('latitude', types.FloatType()),
        types.StructField('longitude', types.FloatType()),
        types.StructField('elevation', types.FloatType()),
        types.StructField('tmax', types.FloatType()),
    ])

    data = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    dayofyear = SQLTransformer(
        statement = """
        SELECT *, dayofyear(date) AS dayofyear 
        FROM __THIS__""")
        
    #yesterday = SQLTransformer(statement = """
     #   SELECT today.latitude, today.longitude, today.elevation, today.tmax, dayofyear(today.date) AS dayofyear, yesterday.tmax AS yesterday_tmax
      #  FROM __THIS__ as today
       # INNER JOIN __THIS__ as yesterday
        #ON date_sub(today.date, 1) = yesterday.date
        #AND today.station = yesterday.station
    #""")

    assembler_features = VectorAssembler(
        inputCols = ['latitude', 'longitude', 'elevation', 'dayofyear'],
        outputCol = 'features')
    #assembler_features = VectorAssembler(
    #   inputCols = ['latitude', 'longitude', 'elevation', 'dayofyear', 'yesterday_tmax'],
    #    outputCol = 'features')
    
    regressor = GBTRegressor(
        featuresCol = 'features',
        labelCol = 'tmax'
    )

    pipeline = Pipeline(stages = [dayofyear, assembler_features, regressor])
    #pipeline = Pipeline(stages = [yesterday, assembler_features, regressor])

    model = pipeline.fit(train)
    predictions = model.transform(validation)
    predictions.show(10)

    r2_evaluator = RegressionEvaluator(
        predictionCol = 'prediction', labelCol = 'tmax', metricName = 'r2'
    )
    r2 = r2_evaluator.evaluate(predictions)

    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)

    print('r2 =', r2)
    print('rmse =', rmse)

    model.write().overwrite().save(model_file)

if __name__ == '__main__':
    spark = SparkSession.builder.appName('weather train').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    assert spark.version >= '2.4' # make sure we have Spark 2.4+
    input = sys.argv[1]
    output = sys.argv[2]
    main(input, output)