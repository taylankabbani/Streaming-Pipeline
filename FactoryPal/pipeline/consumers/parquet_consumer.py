from context import ConsumerSparkABS
from pyspark.sql.types import StructType, StructField, FloatType, TimestampType, IntegerType


class ParquetConsumerSpark(ConsumerSparkABS):
    def __init__(self, spark_master: str, bootstrap_server: str, memory: int, cores: int):
        super().__init__(spark_master, bootstrap_server, memory, cores)
        self.parquet_output_paths = ["/home/database/metrics", "/home/database/workorder"]
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
    
    def query_writer(self,df, i, topic):
        query = df.writeStream \
            .outputMode("append") \
            .option("checkpointLocation", f"/tmp/checkpoint_{topic}") \
            .option("path", self.parquet_output_paths[i]) \
            .option("maxRecordsPerFile", 10000) \
            .trigger(processingTime=f'{self.sec} seconds') \
            .format("parquet") \
            .start()
        return query
        