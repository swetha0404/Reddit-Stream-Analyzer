import praw
from confluent_kafka import Producer

reddit = praw.Reddit(
    client_id="DZS3_bE4g2g8FNb2U_L4xg",
    client_secret="00SS84JvkfzCKbBC__G-GDDLQKjruA",
    user_agent="macos:com.example.myredditapp:v0.1 (by u/MaxToFerrari)",
)

p = Producer({'bootstrap.servers': "localhost:9092"})

# Kafka topic to write messages to
topic_name = "reddit_producer"

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


for submission in reddit.subreddit("all").stream.submissions(skip_existing=True):
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message. The delivery report callback will
    # be triggered from the call to poll() above, or flush() below, when the
    # message has been successfully delivered or failed permanently.
    data = submission.title
    #if (len(submission.selftext) < 500):
    data += " " + submission.selftext
    print(data)
    p.produce(topic_name, data.encode('utf-8'), callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()