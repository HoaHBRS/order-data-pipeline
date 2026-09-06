import json
import unittest
from unittest.mock import MagicMock, Mock, patch
from kafka_consumer import create_consumer, consume_orders

class KafkaConsumerTests(unittest.TestCase):

    @patch("kafka_consumer.Consumer")
    def test_create_consumer_uses_reliable_config(
        self,
        mock_consumer_class,
    ):
        create_consumer()

        mock_consumer_class.assert_called_once_with({
            "bootstrap.servers": "localhost:9092",
            "group.id": "order-pipeline-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })


    @patch("kafka_consumer.initialize_database")
    @patch("kafka_consumer.create_database_connection")
    @patch("kafka_consumer.create_consumer")
    def test_consume_orders_subscribes_and_closes(
        self,
        mock_create_consumer,
        mock_create_database_connection,
        mock_initialize_database,
    ):
        consumer = Mock()
        consumer.poll.side_effect = KeyboardInterrupt
        mock_create_consumer.return_value = consumer

        connection = Mock()
        mock_create_database_connection.return_value = connection

        consume_orders()

        consumer.subscribe.assert_called_once_with(["orders.raw"])
        mock_initialize_database.assert_called_once_with(connection)
        consumer.close.assert_called_once_with()
        connection.close.assert_called_once_with()


    @patch("kafka_consumer.load_rejected_orders")
    @patch("kafka_consumer.load_orders")
    @patch("kafka_consumer.validate_orders")
    @patch("kafka_consumer.create_database_connection")
    @patch("kafka_consumer.create_consumer")
    def test_consume_orders_commits_valid_message(
        self,
        mock_create_consumer,
        mock_create_database_connection,
        mock_validate_orders,
        mock_load_orders,
        mock_load_rejected_orders,
    ):
        order = {
            "order_id": "ORD-CONSUMER-1",
            "customer_id": "C-TEST",
            "amount_cents": "4990",
            "status": "PENDING",
            "updated_at": "2026-08-01T10:00:00",
        }

        validated_order = {"order_id": "ORD-CONSUMER-1", "validated": True}
        mock_validate_orders.return_value = ([validated_order], [])

        message = Mock()
        message.error.return_value = None
        message.value.return_value = json.dumps(order).encode("utf-8")

        consumer = Mock()
        consumer.poll.side_effect = [message, KeyboardInterrupt]
        mock_create_consumer.return_value = consumer

        connection = MagicMock()
        mock_create_database_connection.return_value = connection

        consume_orders()

        mock_validate_orders.assert_called_once_with([order])
        mock_load_orders.assert_called_once_with(connection, [validated_order])
        mock_load_rejected_orders.assert_called_once_with(connection, [])

        consumer.commit.assert_called_once_with(
            message=message,
            asynchronous=False,
        )


    @patch("kafka_consumer.load_orders")
    @patch("kafka_consumer.create_database_connection")
    @patch("kafka_consumer.create_consumer")
    def test_consume_orders_does_not_commit_when_database_load_fails(
        self,
        mock_create_consumer,
        mock_create_database_connection,
        mock_load_orders,
    ):
        order = {
            "order_id": "ORD-CONSUMER-1",
            "customer_id": "C-TEST",
            "amount_cents": "4990",
            "status": "PENDING",
            "updated_at": "2026-08-01T10:00:00",
        }

        message = Mock()
        message.error.return_value = None
        message.value.return_value = json.dumps(order).encode("utf-8")

        consumer = Mock()
        consumer.poll.return_value = message
        mock_create_consumer.return_value = consumer

        connection = MagicMock()
        mock_create_database_connection.return_value = connection

        mock_load_orders.side_effect = RuntimeError(
            "Database load failed"
        )

        with self.assertRaises(RuntimeError):
            consume_orders()

        consumer.commit.assert_not_called()
        consumer.close.assert_called_once_with()
        connection.close.assert_called_once_with()



if __name__ == "__main__":
    unittest.main()