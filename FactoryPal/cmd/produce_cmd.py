from context import metrics_producer, workorder_producer
import threading

BOOTSTRAP_SERVER = "192.168.0.3:9092"

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