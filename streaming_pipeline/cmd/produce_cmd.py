from context import metrics_producer, workorder_producer, KafkaAdmin
import threading

BOOTSTRAP_SERVER = "192.168.0.3:9092"
TOPICS_TO_REGISTER = ["metrics", "workorder"]

# create admin instance
admin = KafkaAdmin(bootstrap_servers=BOOTSTRAP_SERVER)

# create kafka topics if dose not exits
for topic in TOPICS_TO_REGISTER:
    if not admin.topic_exists(topic):
        admin.create_topic(topic)

# create producers
metric_producer = metrics_producer.MetricsProducer(
    bootstrap_server=BOOTSTRAP_SERVER)

workorder_producer = workorder_producer.WorkorderProducer(
    bootstrap_server=BOOTSTRAP_SERVER)

# # create threads
metric_thread = threading.Thread(target=metric_producer.run)
workorder_thread = threading.Thread(target=workorder_producer.run)

# start threads
metric_thread.start()
workorder_thread.start()

# wait for threads to finish
metric_thread.join()
workorder_thread.join()