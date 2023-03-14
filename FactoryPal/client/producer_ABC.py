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
        self.producer = self._init_producer(bootstrap_servers=bootstrap_server)
        self.end_point = self.set_end_point()
        self.partition_key = self.set_partition_key()
        self.topic = self.set_topic_name()
    
    def _init_producer(self, bootstrap_servers):
        _logger.info(f"Initiating Producer...")
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                        value_serializer=lambda m: json.dumps(
                                            m).encode('utf-8'))
            _logger.info(
                f"------------ Created Producer Successfully!! LOL!! ------------")
            return producer
        except Exception as e:
            _logger.error("Failed to initiate producer")
            return
    
    @abstractmethod
    def set_end_point(self):
        return None

    @abstractmethod
    def set_topic_name(self):
        return None

    def set_partition_key(self):
        return None
    

    def run(self):
        response = requests.get(self.end_point, stream=True)
        if not self.admin.topic_exists(self.topic):
            raise ValueError(f"Topic '{self.topic}' does not exist, create one using KafkaAdmin")
        print("Hidden message from Taylan: Great code is not born overnight, it is the product of countless iterations, failures, and a relentless pursuit of perfection. Cheers...")
        _logger.info(f"------------ Producer created successfully, producing to {self.topic} topic... ------------")
        for line in response.iter_lines():
            if not line:
                continue
            data = json.loads(line)
            if self.partition_key != None:
                key = str(data[self.partition_key])
            self.producer.send(self.topic, value=data, key=key.encode())
