import json
import time

from kafka import KafkaProducer
from configparser import ConfigParser

config = ConfigParser()

config.read("./conf/settings.ini")

KAFKA_BROKER = config.get("KAFKA", "KAFKA_BROKER")
ORDER_KAFKA_TOPIC = config.get("KAFKA", "ORDER_KAFKA_TOPIC")
ORDER_LIMIT = int(config.get("KAFKA", "ORDER_LIMIT"))


PRODUCER = KafkaProducer(bootstrap_servers=[KAFKA_BROKER],)

class OrderProducer:
    """ Order producer class """

    print("Going to send order details to kafka topic: ", ORDER_KAFKA_TOPIC)

    for i in range(1, ORDER_LIMIT):
        order = {
            "order_id": i,
            "order_date": time.strftime("%Y-%m-%d %H:%M:%S"),
            "order_amount": i * 100,
            "order_status": "pending",
        }
        print("Sending order details: ", order)
        PRODUCER.send(ORDER_KAFKA_TOPIC, json.dumps(order).encode("utf-8"))
        time.sleep(1)


if __name__ == "__main__":
    OrderProducer()