
import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition


@pytest.mark.usefixtures('kafka_container')
def test_produce_and_consume_kafka_message():
    topic = 'test-topics'
    bootstrap_server = 'localhost:9092'
    producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
    producer.send(topic, b'producer message')
    producer.flush()
    producer.close()

    consumer = KafkaConsumer(bootstrap_servers=[bootstrap_server])
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    consumer.seek_to_beginning()
    assert next(consumer).value.decode("utf-8") == 'producer message'
