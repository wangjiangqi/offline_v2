#!/usr/bin/python
# coding: utf-8

import os
os.environ["SPARK_HOME"] = "D:/soft/spark-3.5.1"
os.environ["HADOOP_HOME"] = "D:/soft/hadoop-3.3.6"
os.environ["HADOOP_USER_NAME"] = "root"

import logging
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test") \
    .master("local[*]") \
    .getOrCreate()

print("Spark version:", spark.version)
spark.stop()


SparkSession.builder()



# class Event_sql():




