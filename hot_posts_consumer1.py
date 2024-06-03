import nltk
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
from pyspark.sql.functions import explode
from confluent_kafka import Producer
import json

nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('maxent_ne_chunker')
nltk.download('words')

# Function to perform NER using NLTK
def extract_entities(text):
    entities = []
    # Tokenize the text
    words = nltk.word_tokenize(text)
    # Tag words with part-of-speech (POS) tags
    tagged_words = nltk.pos_tag(words)
    # Perform named entity chunking
    chunked = nltk.ne_chunk(tagged_words)
    # Extract named entities
    for subtree in chunked:
        if isinstance(subtree, nltk.tree.Tree):
            entity = " ".join([word for word, tag in subtree.leaves()])
            entities.append(entity.lower())
    return entities

# Function to extract NER with ID
def extract_entities_with_id(post_data):
    entities = extract_entities(post_data['title'] + ' ' + post_data['text'])
    modified_lines = []
    if "biden" in entities or "biden" in (post_data['title'] + ' ' + post_data['text']).lower():
        modified_lines.append({"ID":"Biden", "post_data": post_data})
    if "trump" in entities or "trump" in (post_data['title'] + ' ' + post_data['text']).lower():
        modified_lines.append({"ID":"Trump", "post_data": post_data})
    return modified_lines

# Register UDF (User Defined Function) for Spark DataFrame
extract_entities_with_id_udf = udf(extract_entities_with_id, ArrayType(StructType([
    StructField("ID", StringType(), False),
    StructField("post_data", StructType([
        StructField("title", StringType(), False),
        StructField("text", StringType(), False),
        StructField("url", StringType(), False),
        StructField("upvotes", StringType(), False),
        StructField("comments", StringType(), False)
    ]), False)
])))

def send_to_kafka_with_print(batch_df, batch_id):
    print("Sending data to Kafka at batch ID:", batch_id)
    print("Data arrived:", batch_df.collect())
    producer = Producer({'bootstrap.servers': bootstrapServers})
    messages = []
    for row in batch_df.collect():
        ID = row['entity']['ID']
        post_data = row['entity']['post_data']
        # Include keys for the post_data list
        post_data_dict = {
            "title": post_data[0],
            "text": post_data[1],
            "url": post_data[2],
            "upvotes": post_data[3],
            "comments": post_data[4]
        }
        messages.append({"ID": ID, "post_data": post_data_dict})
    batch_message = json.dumps(messages)
    print("Data stored to Kafka:", batch_message)
    producer.produce(output_topic, batch_message.encode('utf-8'))
    producer.flush()

if __name__ == "__main__":
    bootstrapServers = "localhost:9092"
    subscribeType = "subscribe"
    input_topic = "reddit_hot_posts_producer"
    output_topic = "hot_posts_filtered_data_producer"

    spark = SparkSession\
        .builder\
        .appName("Consumer")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("SparkSession initialized.")

    # Create DataSet representing the stream of input lines from Kafka
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, input_topic)\
        .option("startingOffsets", "latest")\
        .load()\
        .selectExpr("CAST(value AS STRING)")

    print("Connected to Kafka topic:", input_topic)

    # Convert JSON string to StructType
    json_schema = StructType([
        StructField("title", StringType(), True),
        StructField("text", StringType(), True),
        StructField("url", StringType(), True),
        StructField("upvotes", StringType(), True),
        StructField("comments", StringType(), True)
    ])
    lines = lines.selectExpr("CAST(value AS STRING)")
    lines = lines.select(from_json(lines.value, json_schema).alias("post_data"))

    print("JSON schema defined.")

    # Extract named entities with ID from post_data
    entities_with_id_df = lines.select(extract_entities_with_id_udf('post_data').alias('entities_with_id'))

    print("Named entities extracted.")

    # Explode the array of entities with ID
    exploded_entities_with_id_df = entities_with_id_df.select(explode('entities_with_id').alias('entity'))

    print("Entities exploded.")

    # Start running the query that sends the modified lines with ID to Kafka
    query = exploded_entities_with_id_df \
        .writeStream \
        .outputMode('append') \
        .foreachBatch(lambda batch_df, batch_id: send_to_kafka_with_print(batch_df, batch_id)) \
        .trigger(processingTime='5 seconds') \
        .start()

    print("Streaming query started.")

    query.awaitTermination()
    
