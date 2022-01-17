#import modules
import time

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover
from pyspark.streaming import StreamingContext

PORT=9993


#create Spark session
appName = "app"
spark = SparkSession \
    .builder \
    .appName(appName) \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9993) \
    .load()





#
#read csv file into dataFrame with automatically inferred schema
tweets_csv = spark.read.csv('dataset/tweets.csv', inferSchema=True, header=True)
tweets_csv.show(truncate=False, n=3)





#select only "SentimentText" and "Sentiment" column,
#and cast "Sentiment" column data into integer
data = tweets_csv.select("SentimentText", col("Sentiment").cast("Int").alias("label"))
data.show(truncate = False,n=5)


#divide data, 70% for training, 30% for testing
dividedData = data.randomSplit([0.7, 0.3])
trainingData = dividedData[0] #index 0 = data training
#testingData = dividedData[1] #index 1 = data testing
train_rows = trainingData.count()
test_rows =1
print ("Training data rows:", train_rows, "; Testing data rows:", 1)


tokenizer = Tokenizer(inputCol="SentimentText", outputCol="SentimentWords")
tokenizedTrain = tokenizer.transform(trainingData)
tokenizedTrain.show(truncate=False, n=5)

swr = StopWordsRemover(inputCol=tokenizer.getOutputCol(),
                       outputCol="MeaningfulWords")
SwRemovedTrain = swr.transform(tokenizedTrain)
SwRemovedTrain.show(truncate=False, n=5)

hashTF = HashingTF(inputCol=swr.getOutputCol(), outputCol="features")
numericTrainData = hashTF.transform(SwRemovedTrain).select(
    'label', 'MeaningfulWords', 'features')
numericTrainData.show(truncate=False, n=3)

lr = LogisticRegression(labelCol="label", featuresCol="features",
                        maxIter=10, regParam=0.01)
model = lr.fit(numericTrainData)
print ("Training is done!")






########################################



def foreach_batch_function(df, epoch_id):
    try:
        text=df.collect()[0].value
        print(text)
        df2 = spark.createDataFrame([{"ItemId": "1", "Label": "0", "SentimentSource": "Sentiment140","SentimentText":text}])
        df2.show()


        testingData = df2
        tokenizedTest = tokenizer.transform(testingData)
        SwRemovedTest = swr.transform(tokenizedTest)
        numericTest = hashTF.transform(SwRemovedTest).select(
            'Label', 'MeaningfulWords', 'features')
        numericTest.show(truncate=False, n=2)

        prediction = model.transform(numericTest)
        predictionFinal = prediction.select(
            "MeaningfulWords", "prediction", "Label")
        predictionFinal.show(n=4, truncate=False)
        correctPrediction = predictionFinal.filter(
            predictionFinal['prediction'] == predictionFinal['Label']).count()
        totalData = predictionFinal.count()
        print("correct prediction:", correctPrediction, ", total data:", totalData,
              ", accuracy:", correctPrediction / totalData)

    except:
        print(ValueError)



lines.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()






