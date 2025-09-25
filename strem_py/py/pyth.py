
import logging
import os


from pyspark.sql import SparkSession

import public_func

spark = (SparkSession.builder
    .appName("HiveExternalTableExample")) \
    .config("hive.metastore.uris", "thrift://cdh02:9083") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold", "104857600") \
    .enableHiveSupport() \
    .getOrCreate();