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


#websites = ["Coursera","edX","Udemy","Dice","NodeFlair"]
websites = ["Udemy"]
for website in websites:
    hdfs_url_path = u.geturlpath(website)
    hdfs_path = u.getdatapath(website)
    print(hdfs_url_path)
    print(hdfs_path)

    list_urls = g.read_url(website,hdfs_url_path,Master_Node,executionDate)
    print(len(list_urls))
    if website == 'Coursera' and len(list_urls) > 0:

        new_list = ['https://www.coursera.org/learn/' + item for item in list_urls]

        data_crawl = C.crawl(new_list)
    elif website == 'Udemy' and len(list_urls) > 0:

        
        data_crawl = U.crawl(list_urls)
 
    elif website == 'edX' and len(list_urls) > 0:

        new_list = ['https://www.edx.org/course' + item for item in list_urls]

        data_crawl = E.crawl(new_list)

    elif website == 'NodeFlair':
        data_crawl = N.crawl()

    elif website == 'Dice':
        
        data_crawl = D.crawl(new_list)

    data_crawl.to_parquet(f'data_crawl_{Master_Node}')

    subprocess.run(['hdfs', 'dfs', '-put',f'./data_crawl_{Master_Node}',f'{hdfs_path}/year={year}/month={month}/day={day}'], capture_output=True, text=True)

