#!/usr/bin/python
# coding: utf-8
import logging
import os
from pyspark.sql import SparkSession


import public_func

os.environ["HADOOP_USER_NAME"] = "root"
logging.basicConfig(
    level=logging.INFO,(asctime)s - %(levelname)s - %(message)s',
handlers=[
    logging.FileHandler('spider_amap_weather.log'),
    logging.StreamHandler()
]
)
logger = logging.getLogger(__name__)