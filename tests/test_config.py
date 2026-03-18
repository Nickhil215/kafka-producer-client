"""Basic unit tests (no real Kafka broker needed)."""

from kafka_producer_client import KafkaProducerConfig


def test_config_defaults():
    cfg = KafkaProducerConfig()
    confluent = cfg.to_confluent_config()
    assert confluent["bootstrap.servers"] == "localhost:9092"
    assert confluent["acks"] == "all"
    assert confluent["enable.idempotence"] == "true"


def test_config_override():
    cfg = KafkaProducerConfig(
        bootstrap_servers="b1:9092,b2:9092",
        acks="1",
        extra={"security.protocol": "SASL_SSL"},
    )
    confluent = cfg.to_confluent_config()
    assert confluent["bootstrap.servers"] == "b1:9092,b2:9092"
    assert confluent["acks"] == "1"
    assert confluent["security.protocol"] == "SASL_SSL"


def test_topic_required():
    cfg = KafkaProducerConfig()
    assert cfg.default_topic is None

    cfg2 = KafkaProducerConfig(default_topic="events")
    assert cfg2.default_topic == "events"
