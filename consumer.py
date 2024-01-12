import re
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

if __name__ == "__main__":
    findspark.init()

    # Config
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("TwitterSparkQuery") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    schema = StructType([StructField("message", StringType())])

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitter") \
        .option("startingOffsets", "latest") \
        .option("header", "true") \
        .load() \
        .selectExpr("CAST(value AS STRING) as message")

    df = df \
        .withColumn("value", from_json("message", schema))

    pre_process = udf(
        lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', x.lower().strip()).split(), ArrayType(StringType())
    )
    df = df.withColumn("cleaned_data", pre_process(df.message)).dropna()

    output_path = "localdata"

    query = df \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", output_path) \
        .option("checkpointLocation", "path/to/checkpoint/directory") \
        .start()

    query.awaitTermination()
