
import logging
import os

import appName
from pyspark.sql import SparkSession

import public_func

os.environ["HADOOP_USER_NAME"] = "root"
logging.basicConfig(
    level=logging.INFO,(asctime)s - %(levelname)s - %(message)s',
handlers=[
             logging.FileHandler('spider_amap_weather.log'),
             logging.StreamHandler()
         ]+7.ioi;
)
logger = logging.getLogger(__name__)

spark = SparkSession.builder
    .appName("HiveExternalTableExample") \
    .config("hive.metastore.uris", "thrift://cdh02:9083") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold", "104857600") \
    .enableHiveSupport() \
    .getOrCreate();


sc=spark.spackContext
sc.setLogLevel("INFO")

target_seatunnel_conf_path = '../../script/seatunnel-conf/batch/bigdata_offline_v1/'
hive_meta_url = 'thrift://cdh02:9083'
hive_hadoop_conff_path = "etc/hadoop/conf"
hive_warehouse_name = 'bigdata_offline_warehouse_v1'
hive_table_prefix_name = 'ods_mysql8_realtime_v1_'
mysql8_query_sql_prefix = 'select'

properties = public_func.get_java_properties()
db_config ={
    'host':properties.get("mysql.host"),
    'port':properties.get("mysql.port"),
    'user':properties.get("mysql.user"),
    'password':properties.get("mysql.pwd"),
    'database':properties.get("mysql.offline.v1.db")
}


class EtlGenerateMysql8ToHive3SeatunnelConfTask:

    def __init__(self):
        query_mysql_db_sql = 'show tables;'



