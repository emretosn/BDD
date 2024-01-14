from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("YahooFinanceStreaming") \
    .getOrCreate()

schema = StructType([
    StructField("Date", TimestampType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
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
movingAvg = df.withWatermark("Date", "1 minute").groupBy(
    window("Date", "5 minutes")).agg(avg("Close").alias("MovingAverage"))

# Start the streaming query
query = movingAvg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
