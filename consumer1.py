import nltk
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
from pyspark.sql.functions import explode, desc
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
    #print(entities, text)
    return entities

# Function to extract NER with ID
def extract_entities_with_id(line):
    # Extract NER from the input line
    entities = extract_entities(line)
    # Initialize a list to store the modified lines with ID
    modified_lines = []
    # Check if "Biden" or "Trump" is in the extracted entities
    if "biden" in entities or "biden" in line.lower():
        # Append (ID, input line) for "Biden" to the modified lines list
        modified_lines.append({"ID":"Biden", "line":line})
    if "trump" in entities or "trump" in line.lower():
        # Append (ID, input line) for "Trump" to the modified lines list
        modified_lines.append({"ID":"Trump", "line":line})
    # Return the modified lines
    return modified_lines

# Register UDF (User Defined Function) for Spark DataFrame
extract_entities_with_id_udf = udf(extract_entities_with_id, ArrayType(StructType([
    StructField("ID", StringType(), False),
    StructField("line", StringType(), False)
])))

def send_to_kafka(batch_df, batch_id):
    producer = Producer({'bootstrap.servers': bootstrapServers})
    messages = []
    for row in batch_df.collect():
        print(f"row {row}")
        ID = row['entity']['ID']  # Accessing the 'ID' field from the StructType
        line = row['entity']['line']  # Accessing the 'line' field from the StructType
        messages.append({"ID": ID, "line": line})
    batch_message = json.dumps(messages)
    
    producer.produce(output_topic, batch_message.encode('utf-8'))
    producer.flush()


if __name__ == "__main__":
    bootstrapServers = "localhost:9092"
    subscribeType = "subscribe"
    input_topic = "reddit_producer"
    output_topic = "filtered_data_producer"

    spark = SparkSession\
        .builder\
        .appName("Consumer")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Create DataSet representing the stream of input lines from Kafka
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, input_topic)\
        .load()\
        .selectExpr("CAST(value AS STRING)")

    # Extract named entities with ID from lines
    entities_with_id_df = lines.select(extract_entities_with_id_udf('value').alias('entities_with_id'))

    # Explode the array of entities with ID
    exploded_entities_with_id_df = entities_with_id_df.select(explode('entities_with_id').alias('entity'))

    # Start running the query that sends the modified lines with ID to Kafka
    query = exploded_entities_with_id_df \
        .writeStream \
        .outputMode('append') \
        .foreachBatch(send_to_kafka) \
        .trigger(processingTime='5 seconds') \
        .start()

    query.awaitTermination()
