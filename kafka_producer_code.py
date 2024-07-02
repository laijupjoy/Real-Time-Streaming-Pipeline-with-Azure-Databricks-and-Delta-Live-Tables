from confluent_kafka import Producer
import random
import time
import json
import logging
import pytz
import yaml

# Read the configuration from the config.yml file
with open('config.yml', 'r') as config_file:
    config = yaml.safe_load(config_file)

logger = logging.getLogger(__name__)
ist_tz = pytz.timezone('Asia/Kolkata')  # Set the timezone to Indian Standard Time

# Set the log formatter to use the IST timezone
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S %Z')
handler = logging.FileHandler('app.log')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Function to generate random data
def generate_random_data():
    # Generate random values based on the schema
    customer_id = 'CUS' + str(random.randint(1, 4000))
    month = random.choice(
        ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November',
         'December'])
    category = random.choice(
        ['Health_Wellness', 'Groceries', 'Electronics', 'Bills', 'Entertainment', 'Apparel', 'Food', 'Others',
         'Travel'])
    payment_type = random.choice(['Credit Card', 'UPI', 'Debit Card', 'Net Banking'])
    spend = random.randint(10, 15000)
    transaction_id = random.randint(1000000000, 9999999999)  # Unique numeric value for primary key

    # Create a data dictionary based on the schema
    data = {
        'customer_id': customer_id,
        'month': month,
        'category': category,
        'payment_type': payment_type,
        'spend': spend,
        'transaction_id': transaction_id
    }

    return data


def delivery_callback(err, msg):
    if err:
        logger.error(f'Error occurred: {err}')
    else:
        key = msg.key().decode('utf-8')
        data = json.dumps(msg.value().decode('utf-8'))
        logger.info(f"{data} successfully pushed to kafka topic [{kafka_topic}]")


class KafkaDataProducer:
    def __init__(self, kafka_topic, bootstrap_servers, username, password):
        self.kafka_topic = kafka_topic
        self.bootstrap_servers = bootstrap_servers
        self.username = username
        self.password = password

        # Create a Kafka producer with authentication
        producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': self.username,
            'sasl.password': self.password,
            'client.id': "vimal-laptop"
        }
        self.producer = Producer(producer_config)

    def generate_and_send_to_kafka(self, duration):
        logger.info(f'Starting to generate and send data to Kafka topic [{self.kafka_topic}] for {duration} seconds')

        counter = 0
        start_time = time.time()
        while time.time() - start_time < duration:
            random_data = generate_random_data()

            # Send data to Kafka
            self.producer.produce(self.kafka_topic, key=str(random_data['transaction_id']),
                                  value=json.dumps(random_data),
                                  callback=delivery_callback)
            self.producer.poll(1)
            counter += 1

        logger.info(f"Total records generated: {counter}")
        logger.info('Flushing and closing the producer')

        # Flush and close the producer
        self.producer.flush(2)

        logger.info('Data generation and sending completed')


# Access the configuration values
duration = config['duration']
kafka_topic = config['kafka_topic']
bootstrap_servers = config['bootstrap_servers']
username = config['username']
password = config['password']

# Example usage or entry point
data_producer = KafkaDataProducer(kafka_topic, bootstrap_servers, username, password)
data_producer.generate_and_send_to_kafka(duration)