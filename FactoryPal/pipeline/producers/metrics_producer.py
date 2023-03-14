from context import ProducerABC

class MetricsProducer(ProducerABC):
    def set_end_point(self):
        end_point = "http://192.168.0.1:5000/stream_data_metrics"
        return end_point

    def set_partition_key(self):
        key = 'id'
        return key
    
    def set_topic_name(self):
        topic = "metrics"
        return topic

if __name__ == "__main__":
    test = MetricsProducer()
    test.run()
