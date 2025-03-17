import configparser
import json

import pika

from common.constants import CONFIG_PATH, TYPE_TOPIC

CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)

HOST = CONFIG.get('default', 'host')
EXCHANGE_NAME = CONFIG.get('default', 'exchange_name')
ROUTING_KEY = CONFIG.get('consumer', 'routing_key')
QUEUE_NAME = CONFIG.get('consumer', 'routing_key')


class Consumer:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST, port=5672))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=TYPE_TOPIC)
        self.channel.queue_declare(queue=QUEUE_NAME)
        self.channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY)

    def __del__(self):
        self.connection.close()

    def listen(self):
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.basic_consume(queue=QUEUE_NAME, on_message_callback=self.callback, auto_ack=True)
        self.channel.start_consuming()

    # listen
    def callback(self, ch, method, properties, body):
        data = json.loads(body)
        print(f" [x] Received: {data['message']}")


if __name__ == '__main__':
    consumer = Consumer()
    consumer.listen()
