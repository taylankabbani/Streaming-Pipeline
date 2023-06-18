from context import ProducerABC

class WorkorderProducer(ProducerABC):
    def set_end_point(self):
        end_point = "http://192.168.0.1:5000/stream_data_workorder"
        return end_point

    def set_partition_key(self):
        key = 'product'
        return key
    
    def set_topic_name(self):
        topic = "workorder"
        return topic    