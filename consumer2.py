from collections import defaultdict
from confluent_kafka import Consumer, KafkaError
import matplotlib.pyplot as plt
from datetime import datetime

plt.ion()

def consume_from_kafka(consumer, topic):
    consumer.subscribe([topic])
    counts = defaultdict(int)
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
                print(f"ID: {msg.value().decode('utf-8')}")
                line = msg.value().decode('utf-8')
                print(line)
                
                # Count mentions of Trump and Biden
                counts['Trump'] += line.count('Trump')
                counts['Biden'] += line.count('Biden')
                print(counts)
                # Plot frequency count over time
                plt.clf()  # Clear the previous plot
                plt.bar(counts.keys(), counts.values())
                plt.xlabel('Politician')
                plt.ylabel('Frequency')
                plt.title('Mentions of Politicians over Time')
                plt.draw()
                plt.pause(0.001)  # Pause for a short time to allow the plot to update
                # Generate timestamp
                timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
                
                # Save the graph with timestamp
                plt.savefig(f'freq_{timestamp}.png')

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