import json
import configparser

import pika

from common.constants import CONFIG_PATH, TYPE_TOPIC

CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)
HOST = CONFIG.get('default', 'host')
EXCHANGE_NAME = CONFIG.get('default', 'exchange_name')
ROUTING_KEY = CONFIG.get('producer', 'routing_key')


class Producer:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST, port=5672))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=TYPE_TOPIC)

    def __del__(self):
        self.connection.close()

    def produce(self):
        while True:
            message = input("Type message: ")
            self.__publish_message(message)

    def __publish_message(self, message):
        message_dict = {
            'message': message
        }
        self.channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=ROUTING_KEY,
            body=json.dumps(message_dict),
        )



if __name__ == '__main__':
    producer = Producer()
    producer.produce()