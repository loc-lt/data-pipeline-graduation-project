import requests
from bs4 import BeautifulSoup
import pandas as pd
import hashlib
from datetime import date
import json
import html2text

import time
from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.support import expected_conditions 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait 
import pyarrow as pa
import pyarrow.csv as pc
import pandas as pd

current = date.today()
executionDate = current.strftime("%Y-%m-%d")

def generate_url_edX():
    totalpage = 5
    list_urls = []
    for i in range(1,int(totalpage)+1):
        PATH_TO_WEBDRIVER = './chromedriver.exe'
        chrome_options = webdriver.ChromeOptions()
        driver = Chrome(PATH_TO_WEBDRIVER)
        seed_url = f'https://www.edx.org/search?tab=course&subject=Computer+Science&subject=Data+Analysis+%26+Statistics&page={i}'
        urls = []
        links = []
        driver.get(seed_url)
        if i != 42:
            total = 24
            xpath = f"//*[@id='main-content']/div/div[4]/div[2]/div[{total}]"
            WebDriverWait(driver, 20).until(expected_conditions.visibility_of_element_located((By.XPATH, xpath)))
        else:
            driver.implicitly_wait(10)
        elements = driver.find_elements_by_xpath('//*[@id="main-content"]/div/div[4]/div[1]/nav/ul/li[6]/button')
        if elements:
            # Lấy thẻ span cuối cùng từ danh sách
            last_span = elements[-1]
            # Lấy text của thẻ span cuối cùng
            totalpage = last_span.text

        h3_elements = driver.find_elements_by_xpath("//div[contains(@class, 'base-card-wrapper')]")
        print(len(h3_elements))
        # Lặp qua từng phần tử <h3>
        for h3_element in h3_elements:
            # Tìm phần tử <a> trong phạm vi của từng phần tử <h3>
            a_element = h3_element.find_element_by_xpath(".//a")

            # Lấy nội dung của thuộc tính href của phần tử <a>
            href = a_element.get_attribute("href").split('?')[0]
            list_urls.append(href)

        driver.quit()
    df = pd.DataFrame(columns=['hashid', 'link','executionDate'])
    for link in list_urls:
        cleaned_link = link.replace('https://www.edx.org/course', '')

        # Hashing bằng SHA-256
        hasher = hashlib.sha256()
        hasher.update(cleaned_link.encode('utf-8'))
        hash_id = hasher.hexdigest()

        # Thêm dữ liệu vào DataFrame
        df.loc[len(df)] = [ hash_id,cleaned_link,executionDate]
    return df

def generate_url_udemy():
    file_path = "./udemy_category.json"

# Đọc nội dung từ file JSON
    with open(file_path) as json_file:
        data = json.load(json_file)
    list_urls = []
    for key, values in data.items():
        sub1 = key
        sub2 = values 
        PATH_TO_WEBDRIVER = './chromedriver.exe'
        driver = Chrome(PATH_TO_WEBDRIVER)
        #user_agent="114.0.0.0"
        #chrome_options = webdriver.ChromeOptions().add_argument(f"user-agent={user_agent}")
        seed_url = f'https://www.udemy.com/courses/development/data-science/?p={i}&sort=newest'

        driver.get(seed_url)

    
        xpath = "//h3[contains(@class, 'ud-heading-md course-card--course-title--vVEjC') and @data-purpose='course-title-url']"
        WebDriverWait(driver, 20).until(expected_conditions.visibility_of_element_located((By.XPATH, xpath)))
        #time.sleep(1)
        h3_element = driver.find_element_by_xpath("//div[@class='course-list--container--FuG0T']")
        soup = BeautifulSoup(h3_element.get_attribute("innerHTML"))
        # #driver.quit()
        h3_elements = soup.find_all("h3",class_ = 'ud-heading-md course-card--course-title--vVEjC',attrs={'data-purpose': 'course-title-url'})

        driver.quit()

        rows = []
        for h3_element in h3_elements:
            row = []
            link = (h3_element.find("a").get('href')).replace("/course/","")

            hasher = hashlib.sha256()
            hasher.update(link.encode('utf-8'))
            hash_id = hasher.hexdigest()

            row.append(hash_id)
            row.append(link)
            row.append(executionDate)
            span_time = h3_element.find("span",attrs={'data-purpose': 'seo-content-info'}).text
            if span_time.find("questions") ==-1:
                row.append(span_time.split(' ')[0])
            else:
                row.append("0")
            row.append(h3_element.find("span",attrs={'data-purpose': 'seo-instructional-level'}).text)
            #row.append(h3_element.find_all("span",attrs={'data-purpose': 'seo-current-price'}))
            rows.append(row)

        df = pd.DataFrame(rows,columns=['hashid', 'link','executionDate','time','level'])
        results = pd.concat([results,df])


    results1 = results.drop_duplicates()
    df = pd.DataFrame(columns=['hashid', 'link','executionDate','time','level'])
    for link in list_urls:
        # Loại bỏ "https://www.coursera.org/learn/" từ đường liên kết
        cleaned_link = link.replace('https://www.udemy.com/course/', '')

        # Hashing bằng SHA-256
        hasher = hashlib.sha256()
        hasher.update(cleaned_link.encode('utf-8'))
        hash_id = hasher.hexdigest()

        # Thêm dữ liệu vào DataFrame
        df.loc[len(df)] = [ hash_id,cleaned_link,executionDate,time,level]
    return df

def generate_url_coursera(seed_url):
    
    response = requests.get(seed_url)
    xml_data = response.text

    # Phân tích cú pháp XML
    soup = BeautifulSoup(xml_data, 'xml')

    # Lấy tất cả các thẻ <loc>
    loc_tags = soup.find_all('loc')
    list_urls = []
    # In ra các đường liên kết
    for loc in loc_tags:
        list_urls.append(loc.text)
    df = pd.DataFrame(columns=['hashid', 'link','executionDate'])
    for link in list_urls:
        # Loại bỏ "https://www.coursera.org/learn/" từ đường liên kết
        cleaned_link = link.replace('https://www.coursera.org/learn/', '')

        # Hashing bằng SHA-256
        hasher = hashlib.sha256()
        hasher.update(cleaned_link.encode('utf-8'))
        hash_id = hasher.hexdigest()

        # Thêm dữ liệu vào DataFrame
        df.loc[len(df)] = [ hash_id,cleaned_link,executionDate]
    return df

def generate_url_nodeflair():
    url = 'https://nodeflair.com/api/v2/jobs?page='
    end_page = 10
    def fetch_jobs(page):
        response = requests.get(url+str(page))
        return response.json()["job_listings"]
    def fetch_all_jobs():
        jobs = []
        for page in range(1, end_page+1):
            jobs += fetch_jobs(page)
        return jobs
    def write_to_JSON_file():
        jobs = fetch_all_jobs()
        with open('jobs.json', 'w') as outfile:
            json.dump(jobs, outfile)
    write_to_JSON_file()
    df_nodeflair = pd.read_json('jobs.json')
    list_urls = df_nodeflair['job_path'].tolist()

    df = pd.DataFrame(columns=['hashid', 'link','executionDate'])
    for link in list_urls:
        
        

        # Hashing bằng SHA-256
        hasher = hashlib.sha256()
        hasher.update(link.encode('utf-8'))
        hash_id = hasher.hexdigest()

        # Thêm dữ liệu vào DataFrame
        df.loc[len(df)] = [ hash_id,link,executionDate]
    return df

def generate_url_dice():

    list_urls = []
    domain_name = 'https://www.dice.com'

    ## Input list keyword
    Key_work_list = ["Backend", "Frontend", "Fullstack", "DevOps", "Data Scientist", "Data Engineer", "Data Analyst", "AI Engineer", "iOS", "Android", "Cybersecurity Engineer", "Cybersecurity Operations", "Blockchain"]

    ## List domain
    domain_list = []
    for key_word in Key_work_list:
        key_main=domain_name+'/jobs/'+'q-'+key_word+'-l--radius-20-startPage-1-jobs?'
        domain_list.append(key_main)

    all_herfs = {}
    for key_word in Key_work_list:
        all_herfs[key_word] = []
    index_list = []
    for index, link in enumerate(domain_list): #
        #print('--' + Key_work_list[index] + '--')
        page=0
        for i in range(3): #pages_for_keyword[index]-10
            page = i+1
            response_page = requests.get(domain_name+'/jobs/'+'q-'+Key_work_list[index]+'-l--radius--startPage--jobs?p='+str(page))
            #print(response_page)
            soup1 = BeautifulSoup(response_page.text,'lxml')
            # Every page contains 20 urls
            for j in range(20):
                tmp = domain_name + soup1.find("a",{"id":"position"+str(j)}).get('href')
                list_urls.append(tmp)

    df = pd.DataFrame(columns=['hashid', 'link','executionDate',"index"])
    for link,index in list_urls,index_list:
        # Hashing bằng SHA-256
        hasher = hashlib.sha256()
        hasher.update(link.encode('utf-8'))
        hash_id = hasher.hexdigest()

        # Thêm dữ liệu vào DataFrame
        df.loc[len(df)] = [ hash_id,link,executionDate,index]
    return df

def generate_url(website):
    if website == 'Coursera':
        seed_url = 'https://www.coursera.org/sitemap~www~courses.xml'
        link_coursera = generate_url_coursera(seed_url)
        return link_coursera
    if website == 'edX':
        link = generate_url_edX()
        return link
    if website == 'edemy':
        link = generate_url_udemy()
        return link
    if website == 'NodeFlair':
        link_nodeflair = generate_url_nodeflair()
        return link_nodeflair
    if website == 'Dice':
        link_dice = generate_url_nodeflair()
        return link_dice


def read_url(website,hdfs_path,master_node,executiondate):
    if website == 'Udemy':
        fs = pa.hdfs.connect()



        file_list = fs.ls(hdfs_path)
        csv_files = [file_name for file_name in file_list if file_name.endswith(".csv")]

        dataframes = pd.DataFrame(columns=['hashid', 'link', 'time', 'level', 'executionDate', 'Master_Node'])

        for file_name in csv_files:
            
            hdfs_file_path =  file_name

            with fs.open(hdfs_file_path, 'rb') as file:
        
                table = pa.csv.read_csv(file, pa.csv.ReadOptions(column_names=['hashid', 'link', 'time', 'level', 'executionDate', 'Master_Node']))
                df = table.to_pandas()
        
                dataframes =pd.concat([dataframes,df])
            
        dataframes['executionDate'] = dataframes['executionDate'].astype('str')
        filtered_df = dataframes.loc[(dataframes['executionDate'] == executiondate) & (dataframes['Master_Node'] == master_node) ]

        #listurls = filtered_df['link'].tolist()
        return filtered_df
    else:
        fs = pa.hdfs.connect()
        file_list = fs.ls(hdfs_path)
        csv_files = [file_name for file_name in file_list if file_name.endswith(".csv")]

        dataframes = pd.DataFrame(columns=['hashid', 'link', 'executionDate', 'Master_Node'])

        for file_name in csv_files:
            
            hdfs_file_path =  file_name

            with fs.open(hdfs_file_path, 'rb') as file:
        
                table = pa.csv.read_csv(file, pa.csv.ReadOptions(column_names=['hashid', 'link',  'executionDate', 'Master_Node']))
                df = table.to_pandas()
        
                dataframes =pd.concat([dataframes,df])
            
        dataframes['executionDate'] = dataframes['executionDate'].astype('str')
        filtered_df = dataframes.loc[(dataframes['executionDate'] == executiondate) & (dataframes['Master_Node'] == master_node) ]

        listurls = filtered_df['link'].tolist()
        return listurls
