import json
from confluent_kafka import Consumer
from pipeline import (
    DB_PATH,
    create_database_connection,
    initialize_database,
    load_orders,
    load_rejected_orders,
    validate_orders,
)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders.raw"
GROUP_ID = "order-pipeline-consumer"


def create_consumer():
    return Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

def consume_orders():
    consumer = create_consumer()

    connection = create_database_connection(DB_PATH)
    initialize_database(connection)

    consumer.subscribe([TOPIC])

    try:
        while True:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                print(f"Consumer error: {message.error()}")
                continue

            order = json.loads(
                message.value().decode("utf-8")
            )

            valid_orders, rejected_orders = validate_orders([order])

            with connection:
                load_orders(connection, valid_orders)
                load_rejected_orders(connection, rejected_orders)

            consumer.commit(
                message=message,
                asynchronous=False,
            )

    except KeyboardInterrupt:
        print("Consumer stopped.")

    finally:
        consumer.close()
        connection.close()


def main():
    consume_orders()


if __name__ == "__main__":
    main()