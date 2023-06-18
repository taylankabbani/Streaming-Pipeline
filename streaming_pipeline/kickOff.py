""""The main script to kick off the streaming_pipeline Data Pipeline"""

import docker
import time
import os

__author__ = "Taylan Kabbani"
__date__ = "14/03/2023"


if __name__ == "__main__":
    client = docker.from_env()

    # get container list:
    containers_ls = client.containers.list()
    assert len(containers_ls) != "It seems like you forgot to build your containers. LOL!"
    assert len(containers_ls) == 6, "A container is missing, rebuild again... and come back :)"
    
    
    master = client.containers.get('spark_master')
    print("Installing requirements...")
    # Download required packages
    exit_code, output = master.exec_run('pip install -r requirements.txt',
                             workdir="/home/pipeline/")
    print(output.decode('utf-8'))
    time.sleep(3)
    # Kick of producers
    master.exec_run('python produce_cmd.py &', workdir="/home/cmd/", detach=True)
    
    print("------------- Producer initiated successfully -------------")
    
    time.sleep(5)
    
    _, logs = master.exec_run('python parquet_consume_cmd.py', workdir="/home/cmd/", detach=False, stream=True)
    for log in logs:
        print(log)
    
    # _, logs = master.exec_run('python consul_consume_cmd.py', workdir="/home/cmd/", detach=False, stream=True)
    # for log in logs:
    #     print(log)