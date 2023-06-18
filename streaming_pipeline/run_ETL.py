""""The main script to run the ETL to get product correlated metrics"""

import docker
import time
import os

__author__ = "Taylan Kabbani"
__date__ = "14/03/2023"


if __name__ == "__main__":
    client = docker.from_env()
    master = client.containers.get('spark_master')
    # Download required packages
    _, logs = master.exec_run('python correlation_report.py',
                             workdir="/home/pipeline/ETL", detach=False, stream=True)
    
    for log in logs:
        print(log)
    

