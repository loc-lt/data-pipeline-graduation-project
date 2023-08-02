from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
from datetime import date
import utils as u
from bisect import bisect_left
import pandas as pd
import dictionary as d
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
from cassandra.cluster import Cluster

current = date.today()
executionDate = current.strftime("%Y-%m-%d")
year = executionDate.split('-')[0]
month = executionDate.split('-')[1]
day = executionDate.split('-')[2]
 
spark = SparkSession.builder \
    .master("spark://192.168.59.133:7077") \
    .appName("Batch Processing") \
    .getOrCreate()

websites = ["Coursera","edX","Udemy"]
websites = ["Udemy"]
for website in websites:
    
    hdfs_path = u.getdatapath(website)
    courses = spark.read.option("header",True).parquet(f'hdfs:///Courses/Udemy/year={year}/month={month}/day={day}/')
    courses = courses.withColumn("rating",courses.rating.cast(FloatType()))\
        .withColumn("enroll",courses.enroll.cast(IntegerType()))\
        .withColumn("time",courses.time.cast(IntegerType()))\
        .withColumn("fee",courses.fee.cast(FloatType()))

    

    courses1 = courses.withColumn("skill",f.lower(courses.skillsgain)).drop(f.col("skillsgain"))

    courses1_process = courses1.where(courses1.skill.isNotNull())
    courses1_notprocess = courses1.where(~courses1.skill.isNotNull())
    courses1_notprocess = courses1_notprocess.withColumn("programming_language",f.lit(None).cast("string")) \
                    .withColumn("tool",f.lit(None).cast("string")) \
                    .withColumn("framework",f.lit(None).cast("string")) \
                    .withColumn("platform",f.lit(None).cast("string")) \
                    .drop(f.col("skill"))
    
    courses1_process = courses1_process.withColumn("skill", f.regexp_replace("skill", ",", " "))\
                                    .withColumn("skill", f.regexp_replace("skill", ":", ""))

    col = "programming_language"
    dictsearch = d.getDict(col)
    courses1_process = u.etract_skill(courses1_process,col,dictsearch)


    col = "tool"
    dictsearch = d.getDict(col)
    courses1_process = u.etract_skill(courses1_process,col,dictsearch)

    col = "framework"
    dictsearch = d.getDict(col)
    courses1_process = u.etract_skill(courses1_process,col,dictsearch)

    col = "platform"
    dictsearch = d.getDict(col)
    courses1_process = u.etract_skill(courses1_process,col,dictsearch)

    courses1_process = courses1_process.drop(f.col("skill"))
    courses1_processed = courses1_process.unionByName(courses1_notprocess)


    rows = courses1_processed.collect()

    # Tạo pandas DataFrame từ dữ liệu đã thu thập được
    pandas_df = pd.DataFrame(rows, columns=courses1_processed.columns)


    pandas_df["rating"] = pandas_df["rating"].astype('float')
    pandas_df["enroll"] = pandas_df["enroll"].astype('float')
    pandas_df["time"] = pandas_df["time"].astype('float')
    pandas_df["fee"] = pandas_df["fee"].astype('float')


    print(pandas_df)
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()


    key_space = 'datawarehouse'
    u.insert_cassandra_dw_course(session,pandas_df,key_space)
    u.update_subject_enroll(session,pandas_df,key_space)
    u.update_subject_level_time_fee(session,pandas_df,key_space)
    u.update_subject_language_course(session,pandas_df,key_space)
    u.update_subject_framework_course(session,pandas_df,key_space)
    u.update_subject_tool_course(session,pandas_df,key_space)
    u.update_top_tech(session,pandas_df,key_space)

    key_space = website.lower()
    u.insert_cassandra_dw_course(session,pandas_df,key_space)
    u.update_subject_enroll(session,pandas_df,key_space)
    u.update_subject_level_time_fee(session,pandas_df,key_space)
    u.update_subject_language_course(session,pandas_df,key_space)
    u.update_subject_framework_course(session,pandas_df,key_space)
    u.update_subject_tool_course(session,pandas_df,key_space)
    u.update_top_tech(session,pandas_df,key_space)

    cluster.shutdown()

spark.stop()