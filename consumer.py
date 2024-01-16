from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("YahooFinanceStreaming") \
    .getOrCreate()

schema = StructType([
    StructField("Datetime", TimestampType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Adj Close", DoubleType(), True),
    StructField("Volume", IntegerType(), True),
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "yahoo-finance") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as json")
df = df.select(from_json("json", schema).alias("data")).select("data.*")

# Process data with SparkSQL
query1 = "SELECT Date, Close FROM stock_data WHERE Close > 100 ORDER BY Date"
result = spark.sql(query1)


movingAvg = df.withWatermark("Date", "1 minute").groupBy(
    window("Date", "5 minutes")).agg(avg("Close").alias("MovingAverage"))

# Start the streaming query
query1 = result.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query2 = movingAvg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query1.awaitTermination()
query2.awaitTermination()
