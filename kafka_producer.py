import argparse
import json

from confluent_kafka import Producer

from pipeline import extract_orders

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders.raw"


def create_producer():
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
    })

def delivery_report(error, message):
    if error is not None:
        print(f"Delivery failed: {error}")
        return

    print(
        f"Delivered to {message.topic()}"
        f"[{message.partition()}] "
        f"offset={message.offset()}"
    )

def publish_orders(source):
    producer = create_producer()
    orders = extract_orders(source)

    for order in orders:
        order_id = order["order_id"]

        producer.produce(
            topic=TOPIC,
            key=order_id.encode("utf-8"),
            value=json.dumps(order).encode("utf-8"),
            on_delivery=delivery_report,
        )

    producer.flush()

def main():
    parser = argparse.ArgumentParser(
        description="Publish raw orders to Kafka."
    )
    parser.add_argument(
        "source",
        help="CSV path or blob://container/blob-name",
    )
    args = parser.parse_args()
    publish_orders(args.source)

if __name__ == "__main__":
    main()