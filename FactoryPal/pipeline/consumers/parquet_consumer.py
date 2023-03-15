import os
from context import ConsumerSparkABS
from pyspark.sql.types import StructType, StructField, FloatType, TimestampType, IntegerType


class ParquetConsumerSpark(ConsumerSparkABS):
    def __init__(self, spark_master: str, bootstrap_server: str, memory: int, cores: int):
        super().__init__(spark_master, bootstrap_server, memory, cores)
        self.database_path = "/home/database"
        self.parquet_output_paths = self._init_parquet_folders()
        self.sec = 10
    
    def _init_parquet_folders(self):
        paths = []
        if os.path.exists(self.database_path):
            for topic in self.topics:
                topic_path = os.path.join(self.database_path, topic)
                paths.append(topic_path)
        else:
            print(f"Database not found: {self.database_path}")
            return FileNotFoundError
        assert len(paths) > 0, "You need to create folders under the database dir for each topic"
        return paths
      
    def set_topic_names(self):
        return ["metrics", "workorder"]
    
    def set_topic_schemas(self):
        metrics_schema = StructType([
            StructField("id", IntegerType()),
            StructField("val", FloatType()),
            StructField("time", TimestampType())
        ])
        
        workorder_schema = StructType([
            StructField("time", TimestampType()),
            StructField("product", IntegerType()),
            StructField("production", FloatType())
        ])
        return [metrics_schema, workorder_schema]
    
    def query_writer(self,df, topic, i):
        query = df.writeStream \
            .outputMode("append") \
            .option("checkpointLocation", f"/tmp/checkpoint_{topic}") \
            .option("path", self.parquet_output_paths[i]) \
            .option("maxRecordsPerFile", 10000) \
            .trigger(processingTime=f'{self.sec} seconds') \
            .format("parquet") \
            .start()
        return query
        