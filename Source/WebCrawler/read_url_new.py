from datetime import date
import Generateurl.generateurl as g
import pandas as pd
import HTMLparser.Coursera as C
import HTMLparser.edX as E
import HTMLparser.Udemy as U
import HTMLparser.Dice as D
import HTMLparser.NodeFlair as N
import Util.utils as u
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import socket
import subprocess


def get_ip_address():
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    return ip_address

# Gọi hàm để lấy địa chỉ IP
ip = get_ip_address()

with open('ip_address.txt', 'r') as file:
    lines = file.readlines()

ip_master = lines[0].strip()
ip_slave1 = lines[1].strip()

if ip ==ip_master:
    Master_Node = 'Master'
elif ip==ip_slave1:
    Master_Node = 'Slave1'



current = date.today()
executionDate = current.strftime("%Y-%m-%d")
year = executionDate.split('-')[0]
month = executionDate.split('-')[1]
day = executionDate.split('-')[2]


websites = ["Coursera","edX","Udemy","Dice","NodeFlair"]
#websites = ["Udemy"]
for website in websites:
    hdfs_url_path = u.geturlpath(website)
    hdfs_path = u.getdatapath(website)
    print(hdfs_url_path)
    print(hdfs_path)

    website == 'Udemy'
    fs = pa.hdfs.connect()



    file_list = fs.ls(hdfs_url_path)
    csv_files = [file_name for file_name in file_list if file_name.endswith(".csv")]

    dataframes = pd.DataFrame(columns=['hashid', 'link', 'time', 'level', 'executionDate', 'Master_Node'])

    for file_name in csv_files:
        
        hdfs_file_path =  file_name

        with fs.open(hdfs_file_path, 'rb') as file:
    
            table = pa.csv.read_csv(file, pa.csv.ReadOptions(column_names=['hashid', 'link', 'time', 'level', 'executionDate', 'Master_Node']))
            df = table.to_pandas()
    
            dataframes =pd.concat([dataframes,df])
        
    dataframes['executionDate'] = dataframes['executionDate'].astype('str')
    filtered_df = dataframes.loc[(dataframes['executionDate'] == executionDate) ]
    print(filtered_df)
