import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition


@pytest.mark.usefixtures('kafka_container')
def test_produce_and_consume_kafka_message(kafka_container):
    _test_produce_and_consume_kafka_message(kafka_container.get_bootstrap_server())


@pytest.mark.usefixtures('kafka_container_using_docker_compose')
def test_produce_and_consume_kafka_message_using_docker_compose():
    _test_produce_and_consume_kafka_message('localhost:9092')


def _test_produce_and_consume_kafka_message(bootstrap_server: str):
    topic = 'test-topics'
    producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
    producer.send(topic, b'producer message')
    producer.flush()
    producer.close()

    consumer = KafkaConsumer(bootstrap_servers=[bootstrap_server])
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    consumer.seek_to_beginning()
    assert next(consumer).value.decode("utf-8") == 'producer message'
