from collections import defaultdict
from confluent_kafka import Consumer, KafkaError
import matplotlib.pyplot as plt
import joblib
from datetime import datetime
import re
from string import punctuation

def preProcessor(text):
    text = re.sub(r'(http|ftp|https):\/\/([\w\-]+(?:(?:\.[\w\-]+)+))([\w\-\.,@?^=%&:/\+#]*[\w\-\@?^=%&/\+#])?', ' ', text)
    text = re.sub(r'[' + punctuation + ']', ' ', text)
    text = re.sub(r'#(\w + )', ' ', text)
    text = re.sub(r'@(\w + )', ' ', text)
    return text

# Load the saved model and CountVectorizer configuration
model_and_cv = joblib.load('svm_model_and_cv.pkl')
clf = model_and_cv['model']
cvm = model_and_cv['cv']

# Define a function for preprocessing and prediction
def predict_sentiment_pipeline(text):
    # Preprocess the input text
    preprocessed_text = preProcessor(text)
    
    # Transform the preprocessed text using CountVectorizer
    text_counts = cvm.transform([preprocessed_text])
    
    # Predict the sentiment using the trained SVM model
    sentiment = clf.predict(text_counts)
    
    return sentiment[0]

plt.ion()

def consume_from_kafka(consumer, topic):
    consumer.subscribe([topic])
    sentiment_counts = defaultdict(int)
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                data = msg.value().decode('utf-8')
                data = eval(data)  # Convert string representation of dictionary to dictionary
                print(len(data))
                if not data:
                    continue
                for data_item in data:
                    politician = data_item['ID']
                    line = data_item['line']
                    
                    # Get sentiment and count sentiment mentions
                    sentiment = predict_sentiment_pipeline(line)

                    sentiment_counts[(politician, sentiment)] += 1
                
                # Plot frequency count over time
                plt.clf()  # Clear the previous plot
                
                colors = {'Trump': 'red', 'Biden': 'blue'}
                x_labels_order = ['Biden_negative', 'Biden_neutral', 'Biden_positive', 'Trump_negative', 'Trump_neutral', 'Trump_positive']
                for label in x_labels_order:
                    politician, sentiment = label.split('_')
                    plt.bar(label, sentiment_counts[(politician, sentiment)], color=colors[politician])
                
                plt.xlabel('Politician Sentiment')
                plt.ylabel('Number of posts')
                plt.title('Sentiment Analysis')
                plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels
                plt.tight_layout()  # Adjust layout to prevent overlapping
                plt.draw()
                plt.pause(0.001)  # Pause for a short time to allow the plot to update
                
                # Generate timestamp
                timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
                
                # Save the graph with timestamp
                plt.savefig(f'sentiment_analysis_{timestamp}.png')

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

if __name__ == "__main__":
    bootstrap_servers = "localhost:9092"
    input_topic = "filtered_data_producer"

    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'consumer_group',  
        'auto.offset.reset': 'earliest',
        'max.poll.interval.ms': 600000
    }

    consumer = Consumer(conf)
    consume_from_kafka(consumer, input_topic)
