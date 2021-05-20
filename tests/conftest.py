
import pytest
import rootpath
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from testcontainers.compose import DockerCompose
from testcontainers.core.waiting_utils import wait_container_is_ready
from testcontainers.kafka import KafkaContainer


@pytest.fixture
def kafka_container():
    container = KafkaContainer()
    container.start()
    yield container
    container.stop()


@pytest.fixture
def kafka_container_using_docker_compose():
    get_container()
    yield
    get_container().stop()


@wait_container_is_ready()
def get_container():
    COMPOSE_PATH = '{}/tests'.format(rootpath.detect('.', pattern='tests'))
    compose = DockerCompose(COMPOSE_PATH)
    compose.start()
    bootstrap_server = 'localhost:9092'
    consumer = KafkaConsumer(group_id='test', bootstrap_servers=[bootstrap_server])
    if not consumer.topics():
        raise KafkaError('Unable to connect with kafka container!')
    return compose


