#!/bin/python
import sys
sys.dont_write_bytecode = True
#from pyspark import SparkContext, HiveContext, SparkConf
from pyspark.sql import SparkSession
class create_spark_session:
    def __init__(self, app_name, mode):
        self.app_name = app_name
        self.mode = "local[4]" if mode is None else mode
        print("application name: " + self.app_name)
        print("application run mode: " + self.mode)



    def get_sc_spark(self):
        spark = SparkSession \
            .builder \
            .appName(self.app_name)\
            .master(self.mode)\
            .enableHiveSupport() \
            .getOrCreate()
        #sconf = SparkConf().setAppName(self.app_name).setMaster(self.mode).set("spark.kryoserializer.buffer.max","1024")


        sc = spark.sparkContext
        hc = spark
        hc.sql("set optimize.sort.dynamic.partitionining=true")
        hc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        hc.sql("set hive.exec.dynamic.partition=true")
        hc.sql("set hive.enforce.bucketing=false")
        hc.sql("set hive.vectorized.execution.enabled=true")
        hc.sql("set hive.enforce.sorting=false")
        hc.sql("set mapreduce.map.memory.mb=9000")
        hc.sql("set mapreduce.map.java.opts=-Xmx7200m")
        hc.sql("set mapreduce.reduce.memory.mb=10000")
        hc.sql("set mapreduce.reduce.java.opts=-Xmx10240m")
        hc.sql("set hive.exec.max.dynamic.partitions.pernode=1000")

        print("******************************************************************************************")
        print("Application ID : " + sc.applicationId)
        print("******************************************************************************************")
        return (sc, hc)

