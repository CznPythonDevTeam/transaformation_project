from confluent_kafka import Consumer, KafkaError
from dotenv import  load_dotenv
import os


class Kafka_Connection():
    def __init__(self):
        load_dotenv("./.virsec_kafka_config.env")
        self.servers = os.environ.get('K_service')
        self.group_id = os.environ.get('K_group_id')

    def get_consume_data(self,topic_names=None):

        # Kafka consumer configuration
        conf = {
            'bootstrap.servers': self.servers,  # Kafka broker address
            'group.id': self.group_id,        # Consumer group ID
            'auto.offset.reset': 'earliest'         # Start consuming from the earliest message
        }

        topic_names = ['test-topic_1']

        # Create Kafka consumer
        consumer = Consumer(conf)

        # Subscribe to Kafka topic
        consumer.subscribe(topic_names)

        # Poll for new messages
        try:
            while True:
                msg = consumer.poll(2.0)  # Poll for new messages, with a timeout of 2 second
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, the consumer reached the end of the topic
                        continue
                    else:
                        # Other error
                        print(msg.error())
                        break
                # Print the consumed message
                msg_value = msg.value().decode('utf-8')
                print('Received message: {}'.format(msg_value))
                # return msg_value
        except KeyboardInterrupt:
            # Stop consuming when Ctrl+C is pressed
            consumer.close()

a = Kafka_Connection()
a.get_consume_data()
