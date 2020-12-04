import sys
sys.dont_write_bytecode = True
sys.path.append("src.zip")
import getpass
import argparse, csv, os, string
import pyspark.sql.functions as F
import json
from src.run.create_spark_session import *
from subprocess import Popen, PIPE, STDOUT
from pyspark.sql.types import *
from datetime import datetime


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    requiredNamed = parser.add_argument_group('required named arguments')

    requiredNamed.add_argument('--input_file', help='Please provide input json file [/user/xyz/abc.json]',
                               required=True)

    args = parser.parse_args()
    properties = {}

    try:
        s = create_spark_session("AIF", "local[4]")
    except Exception as e:
        sys.exit(1)
    (sc, spark) = s.get_sc_spark()
    APP_ID = sc.applicationId
    print("APP ID IS"+APP_ID)
    input_feed_list = []


    def validate(entry):
        try:
            entry_json=json.loads(entry)
            return (entry,True)
        except:
            return (entry,False)


    schema = StructType([
        StructField("item", StringType(), True),
        StructField("valid", BooleanType(), True)
    ])

    validated_rdd = sc.textFile(args.input_file).flatMap(lambda x: [validate(x)])
    validated_frame=spark.createDataFrame(validated_rdd,schema)

    invalid_counts=validated_frame.where(validated_frame.valid == False).count()

    if invalid_counts>0:
        print('ERROR: There are invalid records in your input JSON file')
        sys.exit(1)

    def flatmap_row(entry):
        if entry[1]==True:
            entry_j=json.loads(entry[0])
            user_id=entry_j['user']['id']
            time_stamp=datetime.strptime(entry_j['timestamp'],"%d/%m/%Y %H:%M:%S")
            url_level1=entry_j['url'].split('/')[2] if len(entry_j['url'].split('/'))>2 else 'NULL'
            url_level2=entry_j['url'].split('/')[3] if len(entry_j['url'].split('/'))>3 else 'NULL'
            url_level3 = entry_j['url'].split('/')[4] if len(entry_j['url'].split('/'))>4 else 'NULL'
            activity=entry_j['action']
            return [(user_id,time_stamp,url_level1,url_level2,url_level3,activity)]
        else:
            return []


    validated_record_rdd=validated_rdd.flatMap(flatmap_row)

    schema1 = StructType([
        StructField("user_id", StringType(), True),
        StructField("time_stamp", TimestampType(), True),
        StructField("url_level1", StringType(), True),
        StructField("url_level2", StringType(), True),
        StructField("url_level3", StringType(), True),
        StructField("activity", StringType(), True)
    ])

    v_f_frame=spark.createDataFrame(validated_record_rdd,schema1)

    # first table
    print('INFO: Writing first table raw_table.csv directory')
    v_f_frame.coalesce(1).write.csv('raw_table.csv')

    v_f_frame_withdate = v_f_frame.withColumn('time_bucket', F.date_format(v_f_frame['time_stamp'], 'yyyyMMddHH'))

    # second table
    print('INFO: Writing second table aggregated_table.csv directory')
    v_f_frame_withdate.groupBy(v_f_frame_withdate['time_bucket'], v_f_frame_withdate['url_level1'], v_f_frame_withdate['url_level2'],
                             v_f_frame_withdate['activity']).agg(
        F.count(v_f_frame_withdate['activity']).alias('activity_count'),
        F.countDistinct(v_f_frame_withdate['user_id']).alias('user_count')).coalesce(1).write.csv('aggregated_table.csv')

    sc.stop()

#spark-submit --py-files src.zip main.py --mode "insert" --input_file "c://OldComputer//PycharmProjects//spark//src//conf//data.json"