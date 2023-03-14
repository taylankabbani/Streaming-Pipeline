import os
import sys
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)

_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, _dir)

from client import *
from pipeline.producers import metrics_producer, workorder_producer