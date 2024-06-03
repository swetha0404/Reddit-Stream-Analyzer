import praw
from confluent_kafka import Producer
import time
import gpt

text_file_path = f"./reddit_data.txt"
prompt_path = f"./user_prompt.txt"

reddit = praw.Reddit(
    client_id="DZS3_bE4g2g8FNb2U_L4xg",
    client_secret="00SS84JvkfzCKbBC__G-GDDLQKjruA",
    user_agent="macos:com.example.myredditapp:v0.1 (by u/MaxToFerrari)",
)

p = Producer({'bootstrap.servers': "localhost:9092"})

# Kafka topic to write messages to
topic_name = "reddit_producer"

# File path to write the flushed out data
output_file_path = "reddit_data.txt"

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        # Write the delivered message to the output file
        with open(output_file_path, 'a', encoding = 'utf-8') as file:
            file.write(msg.value().decode('utf-8') + '\n')

def filter_for_relevance(data):
    keywords = ["Biden", "Joe Biden", "Donald Trump", "Trump","Trump's", "Biden's"]
    for keyword in keywords:
        if keyword.lower() in data.lower():
            return True
    return False

# Number of loops to run (adjust based on the desired duration)
num_loops = 5  # Run for 5 intervals of 5 minutes each

for i in range(num_loops):
    start_time = time.time()
    for submission in reddit.subreddit("all").stream.submissions(skip_existing=True):
        # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)
        data = submission.title
        
        data += " " + submission.selftext
        # Check if the data is relevant
        if filter_for_relevance(data):
            print(data)
            p.produce(topic_name, data.encode('utf-8'), callback=delivery_report)

        elapsed_time = time.time() - start_time
        if elapsed_time >= 120:  # 5 minutes
            break  # Exit the loop if 5 minutes have elapsed

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()

    # Sleep for 5 minutes before starting the next loop
    if i < num_loops - 1:
        print("Waiting for 5 minutes...")
        response = gpt.text_summarize(text_file_path, prompt_path)
        open(text_file_path, 'w', encoding='latin-1').close()
        time.sleep(30)