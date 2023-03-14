from .kafka_admin import KafkaAdmin
from abc import ABC, abstractmethod
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource
from kafka.errors import UnknownTopicOrPartitionError
import requests
import json
import logging

# Creating a logger object
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

class ProducerABC(ABC):
    """ Abstract class for kafka API producer"""

    def __init__(self, bootstrap_server="192.168.0.3"):
        self.admin = KafkaAdmin(bootstrap_servers=bootstrap_server)
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_server, value_serializer=lambda m: json.dumps(m).encode('utf-8'))
        self.end_point = self.set_end_point()
        self.partition_key = self.set_partition_key()
        self.topic = self.set_topic_name()
        
    @abstractmethod
    def set_end_point(self):
        return None

    @abstractmethod
    def set_topic_name(self):
        return None

    def set_partition_key(self):
        return None
    
    def topic_exists(self):
        try:
            topic_list = self.admin.get_all_topic_names()
            if self.topic in topic_list:
                return True
            else:
                return False
        except ConnectionAbortedError:
            return False

    def run(self):
        response = requests.get(self.end_point, stream=True)
        self.topic_exists()
        if not self.topic_exists():
            raise ValueError(f"Topic '{self.topic}' does not exist, create one using KafkaAdmin")
        _logger.info(f"------------ Producer created successfully, producing to {self.topic} topic... ------------")
        for line in response.iter_lines():
            if not line:
                continue
            data = json.loads(line)
            self.producer.send(self.topic, value=data)
