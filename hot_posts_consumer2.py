from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, regexp_extract, sum as sql_sum
import re
from confluent_kafka import Producer
import json

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("User_Engagement_and_URL_Analysis") \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Define the Kafka parameters
kafka_bootstrap_servers = "localhost:9092"
input_topic = "hot_posts_filtered_data_producer"
engagement_output_topic = "user_engagement_analysis_producer"
url_output_topic = "url_analysis_producer"

# Define the schema for the data
schema = "ID STRING, post_data STRUCT<title: STRING, text: STRING, url: STRING, upvotes: STRING, comments: STRING>"

# Load data from Kafka topic into a DataFrame
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Extract fields from the nested structure
parsed_df = df \
    .selectExpr("CAST(value AS STRING)") \
    .select(explode(from_json(col("value"), "ARRAY<STRING>")).alias("value")) \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# User Engagement Analysis: Convert upvotes and comments to integer type
engagement_df = parsed_df \
    .withColumn("upvotes", col("post_data.upvotes").cast("int")) \
    .withColumn("comments", col("post_data.comments").cast("int"))

# Calculate cumulative sum of upvotes and comments for each ID (Trump/Biden)
engagement_analysis = engagement_df \
    .groupBy("ID") \
    .agg({"upvotes": "sum", "comments": "sum"}) \
    .orderBy("ID")

# URL Analysis: Extract domains from URLs
def extract_domain(url):
    if url:
        # Regular expression to extract domain from URL
        pattern = r"https?://(?:www\.)?([^/]+)"
        matches = re.findall(pattern, url)
        if matches:
            return matches[0]
    return None

# User-defined function (UDF) to extract domain from URL
extract_domain_udf = spark.udf.register("extract_domain", extract_domain)

# Apply UDF to extract domain from URLs
url_analysis = parsed_df \
    .withColumn("domain", extract_domain_udf(col("post_data.url"))) \
    .groupBy("ID", "domain") \
    .count() \
    .orderBy("ID", "count", ascending=False)

# Function to send data to Kafka
def send_to_kafka(topic, data):
    producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})
    producer.produce(topic, json.dumps(data).encode('utf-8'))
    producer.flush()
    print(f"Data sent to Kafka topic '{topic}'")
    print("Data:", data)  # Print the data sent to Kafka

# Start the streaming queries to output user engagement and URL analysis
query1 = engagement_analysis \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda batch_df, batch_id: send_to_kafka(engagement_output_topic, batch_df.toJSON().collect())) \
    .start()

query2 = url_analysis \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda batch_df, batch_id: send_to_kafka(url_output_topic, batch_df.toJSON().collect())) \
    .start()

query1.awaitTermination()
query2.awaitTermination()