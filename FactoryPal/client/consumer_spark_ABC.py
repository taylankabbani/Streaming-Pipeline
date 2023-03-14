from abc import ABC, abstractmethod
import logging
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import from_json, col

# Creating a logger object
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

# setting packages required for the spark docker bitname image
scala_version = '2.12'
spark_version = '3.1.2'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
]

class ConsumerSparkABS(ABC):
    """ Abstract class for kafka consumer using Spark """
    def __init__(self, spark_master:str, bootstrap_server:str, memory:int, cores:int):
        self.spark = self._init_session(spark_master, memory, cores)
        self.bootstrap_server = bootstrap_server
        self.topics = self.set_topic_names()
        self.schemas = self.set_topic_schemas()
        self.consume_every = 10
        # self.parquet_output_paths = parquet_output_paths
        
        # self.schemas = [self.set_schema() for _ in topics]
        # self.sec = 10  # the number of second to trigger the execution
        assert all(schema is not None for schema in self.schemas), "You need to set a schema for your consumer"
        
    @abstractmethod
    def set_topic_schemas(self):
        """Abstract method to set the data schemas"""
        return []
    
    @abstractmethod
    def set_topic_names(self):
        """Abstract method to set Kafka topic names"""
        return []
    @abstractmethod
    def query_writer(self, df):
        """Abstract method to set the query writer"""
        return None
    def _init_session(self, spark_master, memory, cores):
        """ creating Spark session with specified config """
        memory_str = str(memory)+"m"
        cores_str = str(cores)
        conf = SparkConf() \
            .setAppName("SparkConsumer") \
            .setMaster(spark_master) \
            .set("spark.driver.host", "192.168.0.4") \
            .set("spark.jars.packages", ",".join(packages)) \
            .set("spark.executor.memory", memory_str) \
            .set("spark.driver.memory",memory_str) \
            .set("spark.executor.cores", cores_str) \
            .set('spark.cores.max', cores_str)
        
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        _logger.info(f"------------ Successfully initiating Spark session!! LOL!! ------------")
        return spark

    def run(self):
        """The main spark application to execute"""
        assert len(self.topics) > 0, "The List of Kafka topic names is empty in the consumer"
        assert len(self.schemas) >0, "The List of data schemas is empty in the consumer"
        # will hold the the streams from different topics
        streams = []
        for i, topic in enumerate (self.topics):
            _logger.info(f"------------ Start Consuming from {topic} topic ------------")
            # Subscribe to a topic and created unbounded df
            df = self.spark.readStream \
                .format("Kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_server) \
                .option("subscribe", topic) \
                .option("startingOffsets", "earliest") \
                .option("maxOffsetsPerTrigger", self.consume_every) \
                .load() \
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                .select(from_json(col("value"), self.schemas[i]).alias("parseValue")) \
                .select("parseValue.*")
            query = self.query_writer(df)
            streams.append(query)
        
        for query in streams:
            query.awaitTermination()                        
                