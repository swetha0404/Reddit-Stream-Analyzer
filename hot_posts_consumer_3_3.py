import json
import matplotlib.pyplot as plt
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

# Kafka parameters
bootstrap_servers = 'localhost:9092'
group_id = 'url_analysis_consumer_group'
url_topic = 'url_analysis_producer'  # Topic for URL analysis data

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'latest'
}

# Create Kafka consumer
consumer = Consumer(consumer_config)
consumer.subscribe([url_topic])

# Dictionary to store data for plotting
data_dict = defaultdict(int)

# Main loop to consume data from Kafka and plot URL analysis bar graph using Matplotlib
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

        data = json.loads(msg.value())
        print(data)
        # Process URL analysis data
        for item in data:
            # Assuming the item structure is {'ID': 'Biden', 'domain': 'example.com', 'count': 10}
            # If the structure is different, adjust the code accordingly
            item_data = json.loads(item)
            domain = item_data['domain']
            count = item_data['count']
            data_dict[domain] += count

        # Plot bar graph
        plt.figure(figsize=(10, 6))
        plt.bar(data_dict.keys(), data_dict.values(), color=['blue', 'red'])
        plt.xlabel('Organization Name')
        plt.ylabel('Count')
        plt.title('URL Analysis')
        plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels
        plt.tight_layout()  # Adjust layout to prevent overlapping
        plt.draw()
        plt.pause(0.001)  # Pause for a short time to allow the plot to update
        # Generate timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        
        # Save the graph with timestamp
        plt.savefig(f'3_3_{timestamp}.png')

except KeyboardInterrupt:
    pass

finally:
    consumer.close()