import os
from configparser import ConfigParser
#import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics


#conf = SparkConf().setAll([("spark.app.name","starter"),("spark.proxy.user","robot")])
#spark = SparkSession.builder.appName("starter").master("local[2]").config(conf).getOrCreate()
from pyspark.sql.types import StructField, IntegerType, StringType, StructType

if __name__ == "__main__":
        os.environ['HADOOP_HOME'] = 'G:\softwares\spark-3.0.1\winutils'

        def get_spark_app_config():
            conf = SparkConf()
            conf.set("spark.app.name","sparkmeasure")
            conf.set("spark.master","local[3]")
            conf.set("spark.sql.shuffle.partitions","2")
            #config_parser = ConfigParser()
            #config_parser.read("spark.conf")
            #for k,v in config_parser.items("SPARK_APP_CONFIGS"):
            #    print(k+" "+v)
            #    conf.set(k,v)
            return conf


        conf = get_spark_app_config()
        spark = SparkSession \
                .builder \
                .config(conf=conf) \
                .getOrCreate()

        stagemetrics = StageMetrics(spark)
        #stagemetrics.runandmeasure(locals(),'spark.sql("select count(*) from range(10) cross join range(10)").show()')

        #stagemetrics.runAndMeasure(spark.sql("select count(*) from range(10) cross join range(10)").show())
        stagemetrics.runandmeasure(spark.sql("select count(*) from range(10) cross join range(10)").show())

