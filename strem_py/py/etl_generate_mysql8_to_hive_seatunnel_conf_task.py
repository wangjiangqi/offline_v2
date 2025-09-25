
import logging
import os

# import appName
from pyspark.sql import SparkSession

import public_func

os.environ["HADOOP_USER_NAME"] = "root"
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
# handlers=[
#              logging.FileHandler('spider_amap_weather.log'),
#              logging.StreamHandler()
#          ]+7.ioi;
# )

logging.basicConfig(
    level=logging.INFO,  # 日志级别
    format='%(asctime)s - %(levelname)s - %(message)s',  # 日志格式
    handlers=[  # 日志输出目标（文件+控制台）
        logging.FileHandler('spider_amap_weather.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

spark = (SparkSession.builder
         .appName("HiveExternalTableExample")) \
    .config("hive.metastore.uris", "thrift://cdh02:9083") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold", "104857600") \
    .enableHiveSupport() \
    .getOrCreate();


sc=spark.spackContext
sc.setLogLevel("INFO")

target_seatunnel_conf_path = '../../script/seatunnel-conf/batch/bigdata_offline_v1/'
hive_meta_url = 'thrift://cdh02:9083'
hive_hadoop_conf_path = "etc/hadoop/conf"
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
        res_db_message = public_func.execute_sql(query_mysql_db_sql,db_config,as_dict=True)
        hive_ddl= []
        seatunnel_conf_list = []
        table_names = []
        for i in res_db_message:
            res=public_func.execute_sql(f"{i['Table_in_realtime_v1']} ;",db_config ,as_dict=True)
            table_names.append(i['Tables_in_realtime_v1'])
            fields = [item['Field'] for item in res]
            hive_column = ','.join(fields)+',ds'
            mysql8_all_table_column = ' string ,'.join(fields)
            mysql8_all_column = ', '.join(fields)

            hive_create_table_ddl = f"""
                create external table if not exists {hive_warehouse_name}.{hive_table_prefix_name}{i['Tables_in_realtime_v1']}(
                    {mysql8_all_table_column} string )
                    PARTITIONED BY (ds STRING)
                    STORED AS PARQUET
                    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/{hive_warehouse_name}/{hive_table_prefix_name}{i['Tables_in_realtime_v1']}/'
                    TBLPROPERTIES(
                        'parquet.compress'='SNAPPY',
                        'external.table.purge' = 'true'
                    );
            """
        hive_ddl.append(hive_create_table_ddl)

        seatunnel_conf = f"""
            
            env {{
                parallelism = 2
                job.mode = "BATCH"
            }}
            
            source{{
                Jdbc{{
                    url = "jdbc:mysql://{db_config.get("host")}:3306/realtime_v1?serverTimezone=GMT%2b8&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true&useSSL=false&allowPublicKeyRetrieval=true"
                    driver ="com.mysql.cj.jdbc.Driver
                    connection_check_timeout_sec = 100
                    user = "root"
                    password = {db_config.get("password")}
                    query = "{mysql8_query_sql_prefix}{mysql8_all_column} ,DATE_FORMAT(NOW(), '%Y%m%d') as df from {db_config.get("database")}.{i['Tables_in_realtime_v1']}"
                    
                }}
            }}
        
            transform {{
                
            
            }}
            sink{{
                Hive{{
                    table_name = {hive_warehouse_name}.{hive_table_prefix_name}{i['Table_in_realtime_v1']}
                    metastore_uri = "{hive_meta_url}"
                    hive.hadoop.conf-path = {hive_hadoop_conf_path}
                    save_mode = "overwrite"
                    partition_by = ["ds"]
                    dynamic_partition = true
                    file_format = "PARQUET"
                    parquet_compress = "SNAPPY"
                    tbl_properties = {{
                    
                    }}
                }}
            }}
        """


