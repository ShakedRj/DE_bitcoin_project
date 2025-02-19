from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql import functions as F
from pyspark.sql.functions import col, explode, from_json,to_timestamp, date_format, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


spark = SparkSession \
.builder \
.master("local[*]") \
.appName("KafkaToSparkToPostgresandS3") \
.config("spark.jars", "/opt/driver/postgresql-42.5.6.jar") \
.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
.config("spark.hadoop.fs.s3a.access.key", "") \
.config("spark.hadoop.fs.s3a.secret.key", "") \
.config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
.getOrCreate()

data_schema = StructType([
    StructField("title", StringType()),
    StructField("source_name", StringType()),
    StructField("date", StringType()),
    StructField("sentiment", StringType())
])

# Read data from Kafka topic
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "news-api-data") \
    .option('startingOffsets', 'latest') \
    .load()\
    
# Deserialize the 'value' column (which is in binary format) into a string, and then parse the JSON
json_df = kafka_stream.selectExpr("CAST(value AS STRING) AS json_data")


# Extract the "data" array from the JSON
json_parsed_df = json_df.select(from_json(col("json_data"), StructType([StructField("data", ArrayType(data_schema))])).\
                                alias("parsed_data"))

# Explode the "data" array into individual rows
flattened_df = json_parsed_df.select(explode(col("parsed_data.data")).alias("news_item"))

# Select the relevant fields from the exploded rows
final_df = flattened_df.select(
    col("news_item.title").alias("title"),
    col("news_item.source_name").alias("source_name"),
    col("news_item.date").alias("date"),
    col("news_item.sentiment").alias("sentiment")
)


final_df = final_df.withColumn("date", regexp_replace("date", "^[A-Za-z]+, ", ""))
final_df = final_df.withColumn("date", date_format(to_timestamp("date", "dd MMM yyyy HH:mm:ss Z"), "dd_MM_yyyy"))
final_df = final_df.withColumn("partition_date",F.col("date"))


# Write data to S3
s3_query = final_df.writeStream \
    .partitionBy("partition_date") \
    .format("parquet") \
    .option("path", "s3a://crypto-news-project/bitcoin_news/") \
    .option("checkpointLocation", "s3a://crypto-news-project/checkpoints/") \
    .outputMode("append") \
    .start()

s3_query.awaitTermination()