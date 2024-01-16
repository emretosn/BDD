from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("YahooFinanceStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
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

# Register the DataFrame as a temporary SQL table
df.createOrReplaceTempView("stock_data")

# Process data with SparkSQL
moving_average = "SELECT window(Datetime, '5 minutes') as Window, AVG(Close) as MovingAverage FROM stock_data GROUP BY window(Datetime, '5 minutes')"
moving_average_result = spark.sql(moving_average)

avg_price = "SELECT window(Datetime, '5 minutes') as Window, AVG((Open + High + Low + Close) / 4) as AvgPrice FROM stock_data GROUP BY window(Datetime, '5 minutes')"
avg_price_result = spark.sql(avg_price)

# Start the streaming query
query1 = moving_average_result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query2 = avg_price_result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query1.awaitTermination()
query2.awaitTermination()
