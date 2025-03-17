import configparser

import pika

from common.constants import CONFIG_PATH, TYPE_TOPIC

CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)

HOST = CONFIG.get('default', 'host')
EXCHANGE_NAME = CONFIG.get('default', 'exchange_name')
ROUTING_KEY = CONFIG.get('consumer', 'routing_key')
QUEUE_NAME = CONFIG.get('consumer', 'routing_key')

# channel
connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST, port=5672))
channel = connection.channel()

# exchange & queue
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=TYPE_TOPIC)
channel.queue_declare(queue=QUEUE_NAME)
channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY)

# listen
def callback(ch, method, properties, body):
    print(f" [x] Received {body}")

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
channel.start_consuming()
