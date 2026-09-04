import json
import unittest
from unittest.mock import Mock, patch

from kafka_producer import create_producer, delivery_report, publish_orders

class KafkaProducerTests(unittest.TestCase):

    @patch("kafka_producer.Producer")
    def test_create_producer_uses_reliable_config(
        self,
        mock_producer_class,
    ):
        create_producer()

        mock_producer_class.assert_called_once_with({
            "bootstrap.servers": "localhost:9092",
            "acks": "all",
            "enable.idempotence": True,
        })

    @patch("kafka_producer.extract_orders")
    @patch("kafka_producer.create_producer")
    def test_publish_orders_sends_and_flushes(
        self,
        mock_create_producer,
        mock_extract_orders,
    ):
        producer = Mock()
        mock_create_producer.return_value = producer

        order = {
            "order_id": "ORD-TEST-1",
            "customer_id": "C-TEST",
            "amount": "49.90",
            "status": "created",
            "updated_at": "2026-08-01T10:00:00",
        }
        mock_extract_orders.return_value = [order]

        source = "data/test-orders.csv"

        publish_orders(source)

        mock_create_producer.assert_called_once_with()
        mock_extract_orders.assert_called_once_with(source)

        producer.flush.assert_called_once_with()

        producer.produce.assert_called_once_with(
            topic="orders.raw",
            key=b"ORD-TEST-1",
            value=json.dumps(order).encode("utf-8"),
            on_delivery=delivery_report,
        )


if __name__ == "__main__":
    unittest.main()