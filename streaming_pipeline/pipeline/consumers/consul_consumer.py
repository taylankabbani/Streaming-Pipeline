from context import ConsumerSparkABS
from pyspark.sql.types import StructType, StructField, FloatType, TimestampType, IntegerType


class ConsulConsumerSpark(ConsumerSparkABS):
    
    def set_topic_names(self):
        return ["metrics"]
    
    def set_topic_schemas(self):
        metrics_schema = StructType([
            StructField("id", IntegerType()),
            StructField("val", FloatType()),
            StructField("time", TimestampType())
        ])
        
        # workorder_schema = StructType([
        #     StructField("time", TimestampType()),
        #     StructField("product", IntegerType()),
        #     StructField("production", FloatType())
        # ])
        return [metrics_schema]
    
    def query_writer(self,df, topic, i):
        query = df.writeStream \
                .outputMode("append") \
                .option("maxRecordsPerFile", 10000) \
                .trigger(processingTime='5 seconds') \
                .format("console") \
                .start()
        return query
        