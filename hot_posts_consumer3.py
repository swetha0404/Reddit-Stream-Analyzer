import time
import json
import matplotlib.pyplot as plt
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

# Kafka parameters
bootstrap_servers = 'localhost:9092'
group_id = 'matplotlib_upvotes_consumer_group'
input_topic = 'user_engagement_analysis_producer'  # Topic for user engagement analysis data


# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'latest'
}

# Create Kafka consumer
consumer = Consumer(consumer_config)
consumer.subscribe([input_topic])

# Dictionary to store data for plotting
upvotes_data = defaultdict(list)
start_time = time.time()  # Get the time when the program started

# Main loop to consume data from Kafka and plot graphs using Matplotlib
try:
    flag = False
    i = 0
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

        data = json.loads(msg.value())  # Parse JSON string to dictionary
        print(data)
        if not data:
            continue
        if not flag:
            flag = True
            start_time = time.time()
        # Process user engagement analysis data
        i += 1
        for item in data:
            item_data = json.loads(item)  # Parse JSON string to dictionary
            ID = item_data['ID']
            time_elapsed = 6 * i
            upvotes_data[ID].append((time_elapsed, item_data['sum(upvotes)']))
        
        # Plot graphs for user engagement
        plt.figure(figsize=(10, 6))
        for ID, values in upvotes_data.items():
            x, y = zip(*values)
            print(x, y)
            plt.plot(x, y, label=ID)
        plt.xlabel('Time (hours since start)')
        plt.ylabel('Number of Upvotes')
        plt.title('User Engagement - Upvotes')
        plt.legend()
        plt.draw()
        plt.pause(0.001)  # Pause for a short time to allow the plot to update
        # Generate timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        
        # Save the graph with timestamp
        plt.savefig(f'3_{timestamp}.png')

except KeyboardInterrupt:
    pass

finally:
    consumer.close()