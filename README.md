# FactoryPal Streaming Pipeline

## APIs

This section describes APIs created to stream data to our streaming system. 

* The APIs will use `192.168.0.1` as IP host & port `5000` on the network.
* Each api will have an end point of `stream_data_{file_name}`
* APIs will stream the data using only Json files as source.
* The APIs will stream each data point (json object) with a time interval.
    
    * By default the delay is set to 1 sec
    * You can change it from the docker-compose file > streaming_api service > change the float in the `python src/app.py {your_input}` > you need to build again
* Data will be streamed according to the time element of each json object.
Two REST APIs will be implemented in a backend service:

1. **Metrics API**: Will stream data from `metrics.json` folder in the data_source dir. Access it using: http://192.168.0.1:5000/stream_data_metrics

2. ** workorder API**: Will stream data from `workorder.json` folder in the data_source dir. Will stream using endpoint. Access it using: http://192.168.0.1:5000/stream_data_workorder



