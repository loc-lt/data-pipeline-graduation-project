from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date
import Generateurl.generateurl as g
import pandas as pd
import subprocess
import HTMLparser.Coursera as C
import HTMLparser.edX as E
import HTMLparser.Udemy as U
import HTMLparser.Dice as D
import HTMLparser.NodeFlair as N
import Util.utils as u



spark = SparkSession.builder \
    .appName("getUrl and save to hadoop") \
    .getOrCreate()

websites = ["Coursera","edX","Udemy","Dice","NodeFlair"]

for website in websites:
    ## Phat sinh url hien tai cua trang web
    hdfs_url_path = u.geturlpath(website)
    if website == 'Coursera':
        current_url = g.generate_url_coursera()
    elif website == 'edX':
        current_url = g.generate_url_edX()
    elif website == 'Udemy':
        current_url = g.generate_url_udemy()
    elif website == 'Dice':
        current_url = g.generate_url_nodeflair()
    elif website == 'NodeFlair':
        current_url = g.generate_url_dice()

    old_url = spark.read.parquet(hdfs_url_path)

    print(current_url)
    list_hash_current_url = current_url['hashid'].tolist()
    filtered_hashids = old_url.select("hashid")\
            .subtract(spark.createDataFrame([(i,) for i in list_hash_current_url], ["hashid"]))
    hashid_new = filtered_hashids.rdd.map(lambda row: row.hashid).collect()
    current_url_new = current_url[current_url['hashid'].isin(hashid_new)]

    i = len(current_url_new) // 2
    df1 = current_url_new[i:]
    df1["Master_Node"] = "Master"
    df2 = current_url_new[i:]
    df2["Master_Node"] = "Slave1"
    current_url_new2 = pd.concat([df1,df2])

    if len(current_url_new2) > 0 :
        spark_df = spark.createDataFrame(current_url_new2)
        old_url = old_url.unionByName(spark_df)
        old_url.write.mode('overwrite').csv(f'{hdfs_url_path}tmp')
        subprocess.run(['hdfs', 'dfs', '-rm','-r','-f', hdfs_url_path], capture_output=True, text=True)
        subprocess.run(['hdfs', 'dfs', '-mv',f'{hdfs_url_path}tmp',hdfs_url_path], capture_output=True, text=True)

spark.stop()

