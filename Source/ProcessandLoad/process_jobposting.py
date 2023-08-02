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

current = date.today()
executionDate = current.strftime("%Y-%m-%d")
year = executionDate.split('-')[0]
month = executionDate.split('-')[1]
day = executionDate.split('-')[2]

spark = SparkSession.builder \
    .master("spark://192.168.59.133:7077") \
    .appName("Batch Processing") \
    .getOrCreate()

websites = ["Dice","NodeFlair"]
for website in websites:
    
    hdfs_path = u.getdatapath(website)
    df = spark.read.option("header",True).parquet(f'{hdfs_path}/year={year}/month={month}/day={day}/')
    df = df.select("job_path","title","position","salary_min","salary_max","country","tech_stacks")\
        .withColumnRenamed("job_path","link")\
        .withColumnRenamed("position","industry")\
        .withColumn("description",f.lit(""))\
        .withColumnRenamed("salary_min","min_salary")\
        .withColumnRenamed("salary_max","max_salary")\
        .withColumnRenamed("tech_stacks","skills")\
        .withColumnRenamed("job_path","link")

    job1_process = df.where(df.skills.isNotNull())
    job1_notprocess = df.where(~df.skills.isNotNull())

    job1_process = job1_process.withColumn("skills", f.regexp_replace("skills", ",", ""))
   
    job1_process = job1_process.withColumn("skills",f.lower(job1_process.skill)).drop(f.col("skill")) \
                                .withColumnRenamed("skills","skill")
    job1_notprocess = job1_notprocess.withColumn("programming_language",f.lit(None).cast("string")) \
                    .withColumn("tool",f.lit(None).cast("string")) \
                    .withColumn("framework",f.lit(None).cast("string")) \
                    .withColumn("platform",f.lit(None).cast("string")) \
                    .drop(f.col("skills"))
    
    col = "programming_language"
    dictsearch = d.getDict(col)
    job1_process = u.etract_skill(job1_process,col,dictsearch)


    col = "tool"
    dictsearch = d.getDict(col)
    job1_process = u.etract_skill(job1_process,col,dictsearch)

    col = "framework"
    dictsearch = d.getDict(col)
    job1_process = u.etract_skill(job1_process,col,dictsearch)

    col = "platform"
    dictsearch = d.getDict(col)
    job1_process = u.etract_skill(job1_process,col,dictsearch)

    job1_process = job1_process.drop(f.col("skill"))
    job1_processed = job1_process.unionByName(job1_notprocess)


    rows = job1_processed.collect()

    # Tạo pandas DataFrame từ dữ liệu đã thu thập được
    pandas_df = pd.DataFrame(rows, columns=job1_processed.columns)

    pandas_df["min_salary"] = pandas_df["min_salary"].astype('float')
    pandas_df["max_salary"] = pandas_df["max_salary"].astype('float')



  
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    u.insert_cassandra_dw_course(session,pandas_df,key_space)

    key_space = 'datawarehouse'
    u.insert_cassandra_dw_jobposting(session,pandas_df,key_space)
    u.update_industry_language_job(session,pandas_df,key_space)
    u.update_industry_framework_job(session,pandas_df,key_space)
    u.update_industry_tool_job(session,pandas_df,key_space)
    u.update_top_tech_job(session,pandas_df,key_space)
    u.update_industry_salary(session,pandas_df,key_space)
    key_space = website.lower()
    
    u.insert_cassandra_dw_jobposting(session,pandas_df,key_space)
    u.update_industry_language_job(session,pandas_df,key_space)
    u.update_industry_framework_job(session,pandas_df,key_space)
    u.update_industry_tool_job(session,pandas_df,key_space)
    u.update_top_tech_job(session,pandas_df,key_space)
    u.update_industry_salary(session,pandas_df,key_space)

    cluster.shutdown()

spark.stop()