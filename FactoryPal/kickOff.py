""""The main script to kick off the FactoryPal Data Pipeline"""

import docker
import time
__author__ = "Taylan Kabbani"
__date__ = "11/03/2023"

client = docker.from_env()
master = client.containers.get('spark_master')

# Download required packages
# output = master.exec_run('pip install -r requirements.txt',
#                          workdir="/home/pipeline/")
# print(output.output.decode('utf-8'))

# time.sleep(5)
# # Kick of producers
# master.exec_run('python cmd/produce.py',
#                 workdir="/home/pipeline/", detach=True)
# Kick of consumers
time.sleep(3)
master.exec_run('python cmd/consume.py',
                workdir="/home/pipeline/", detach=True)
time.sleep(2)
