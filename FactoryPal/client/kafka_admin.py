from kafka.admin import KafkaAdminClient, NewTopic
import logging

# Creating a logger object
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)


class KafkaAdmin:
    def __init__(self, bootstrap_servers):
        """Initialize the Kafka cluster admin and perform different admin functions"""
        self.admin_client = self._connect(bootstrap_servers)
        # self.broker_ids =
        self.default_num_partitions = 3
        self.default_num_replica = 1

    def _connect(self, bootstrap_servers):
        """Create an admin client connection and check connection"""
        try:
            connection = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers)
            _logger.info(
                f"------------ Created Admin Connection to: {bootstrap_servers} ------------")
            return connection
        except Exception as e:
            _logger.info(
                "Failed to create a Kafka Admin client because of", e)
            return connection

    def create_topic(self, topic_name):
        """Create a new Kafka topic with the given name, partitions, replication factor, and configuration"""
        assert topic_name not in self.admin_client.list_topics(
        ), f"{topic_name} Topic already exists, LOL!"
        try:
            topic = NewTopic(name=topic_name, num_partitions=self.default_num_partitions,
                             replication_factor=self.default_num_replica)
            self.admin_client.create_topics([topic])
            _logger.info(
                f"------------ Created {topic_name} topic successfully !!------------")
        except Exception as e:
            _logger.info(
                f"Failed to create {topic_name} topic because of", e)

    def delete_topic(self, topic_name):
        """Delete an existing Kafka topic with the given name"""
        assert topic_name in self.admin_client.list_topics(
        ), f"{topic_name} Topic already does not exists, LOL!"
        try:
            self.admin_client.delete_topics([topic_name])
            _logger.info(
                f"------------ Deleted {topic_name} topic successfully !!------------")
        except Exception as e:
            _logger.info(
                f"Failed to delete {topic_name} topic because of", e)

    def get_all_topic_names(self):
        """Get a list of all topic names in the Kafka cluster"""
        topic_ls = self.admin_client.list_topics()
        return topic_ls


if __name__ == "__main__":
    test = KafkaAdmin(bootstrap_servers="192.168.0.3:9092")
    print(test.get_all_topic_names())
    test.create_topic("metrics")
    test.create_topic("workorder")
    # print(test.get_all_topic_names())
    # test.delete_topic("metrics")
    # test.delete_topic("workorder")
    print(test.get_all_topic_names())
