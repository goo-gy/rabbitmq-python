import json
import configparser

import pika

from common.constants import CONFIG_PATH, TYPE_TOPIC

CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)

HOST = CONFIG.get('default', 'host')
EXCHANGE_NAME = CONFIG.get('default', 'exchange_name')
ROUTING_KEY = CONFIG.get('producer', 'routing_key')

# channel
connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST, port=5672))
channel = connection.channel()

# exchange
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=TYPE_TOPIC)

# message
while True:
    message = input("Type message: ")
    message_dict = {
        'message': message
    }
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=ROUTING_KEY,
        body=json.dumps(message_dict),
    )

connection.close() # todo : class
