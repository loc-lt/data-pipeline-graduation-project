import pandas as pd
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import html2text
import re

def crawl(list_urls,list_indexs):
    Key_work_list = ["Backend", "Frontend", "Fullstack", "DevOps", "Data Scientist", "Data Engineer", "Data Analyst", "AI Engineer", "iOS", "Android", "Cybersecurity Engineer", "Cybersecurity Operations", "Blockchain"]
    for url,index in list_urls,list_indexs:
        response_work = requests.get(url)
        soup = BeautifulSoup(response_work.text,'lxml')
        if soup.find("script", {"id":"__NEXT_DATA__"}) is None:
            return None
        json_object = json.loads(soup.find("script", {"id":"__NEXT_DATA__"}).contents[0])
        job_infor = {}
        job_infor['Link'] = url
        # Job Title
        try:
            job_infor['Title'] = next(v for k,v in json_object.get('props').get('pageProps').get('initialState').get('api').get('queries').items() if 'getJobById' in k).get('data').get('title')
        except:
            job_infor['Title'] = None

        # Industry
        try:
            job_infor['Industry'] = Key_work_list[index]
        except:
            job_infor['Industry'] = None

        # Description
        try:
            html_data = next(v for k,v in json_object.get('props').get('pageProps').get('initialState').get('api').get('queries').items() if 'getJobById' in k).get('data').get('description')
            job_infor['Description'] = '. '.join(html2text.html2text(html_data).replace('\n', '').replace('*', '').strip().split("       "))
        except:
            job_infor['Description'] = None

        # Country
        try:
            job_infor['Country'] = next(v for k,v in json_object.get('props').get('pageProps').get('initialState').get('api').get('queries').items() if 'getJobById' in k).get('data').get('locationDetail').get('locations')[0].get('country')
        except:
            job_infor['Country'] = None

        # Job Skills
        try:
            job_infor['Skills'] = [element.get('name') for element in next(v for k,v in json_object.get('props').get('pageProps').get('initialState').get('api').get('queries').items() if 'getJobById' in k).get('data').get('skills')]
        except:
            job_infor['Skills'] = None

        # Min Max Salary: from compensationDetail
        salary_data_from_compensationDetail = next(v for k,v in json_object.get('props').get('pageProps').get('initialState').get('api').get('queries').items() if 'getJobById' in k).get('data').get('compensationDetail').get('rawText')

        if salary_data_from_compensationDetail is not None:
            salary_data = re.findall(r'\b\d+\b', salary_data_from_compensationDetail.replace(',', ''))
            if len(salary_data) == 0:
                job_infor['Min Salary'] = job_infor['Max Salary'] = None
            elif len(salary_data) == 1:
                job_infor['Min Salary'] = job_infor['Max Salary'] = float(salary_data[0])
            else:
                job_infor['Min Salary'] = float(salary_data[0])
                job_infor['Max Salary'] = float(salary_data[1])
        else:
            job_infor['Min Salary'] = job_infor['Max Salary'] = None

    return job_infor