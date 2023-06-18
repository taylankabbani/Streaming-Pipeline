from context import consul_consumer


# Start the consul consumer
SPARK_MASTER = "spark://192.168.0.4:7077"
BOOTSTRAP_SERVER = "192.168.0.3:9092"
MEMORY = 512
CORES = 1

consul = consul_consumer.ConsulConsumerSpark(spark_master=SPARK_MASTER, 
                                                      bootstrap_server=BOOTSTRAP_SERVER,
                                                      memory=MEMORY,
                                                      cores=CORES)

consul.run()